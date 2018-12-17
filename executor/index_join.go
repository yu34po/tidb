package executor

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/ranger"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"runtime"
	"sort"
	"sync"
	"unsafe"
)

var _ Executor = &IndexJoin{}

type IndexJoin struct {
	baseExecutor

	resultCh   <-chan *indexJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerCtx outerCtx
	innerCtx innerCtx

	joinResultCh chan *indexJoinWorkerResult
	taskCh       chan *indexJoinTask

	joinResult *chunk.Chunk
	innerIter  chunk.Iterator

	joiner joiner

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int

	memTracker *memory.Tracker // track memory usage.
	prepare    bool
	workerCtx  context.Context

	closeCh chan struct{} // closeCh add a lock for closing executor.
}
type outerCtx struct {
	rowTypes []*types.FieldType
	keyCols  []int
	joinKeys []*expression.Column

	filter    expression.CNFExprs
	keepOrder bool
}

type innerCtx struct {
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	keyCols       []int
	joinKeys      []*expression.Column

	compareFuncs []chunk.CompareFunc
}

type indexJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool

	outIter  *chunk.Iterator4Chunk
	outerRow chunk.Row

	lookupMap     *mvmap.MVMap
	matchKeyMap   *mvmap.MVMap
	matchedOuters []chunk.Row

	cursor   int
	hasMatch bool

	memTracker *memory.Tracker // track memory usage.

	buildError error
	//merge join use
	joinResultCh chan *indexJoinWorkerResult
}

type indexJoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type IndexHashJoin struct {
	IndexJoin
	joinResultCh      chan *indexJoinWorkerResult
	joinChkResourceCh []chan *chunk.Chunk
}

type IndexMergeJoin struct {
	IndexJoin
}
type outerWorker struct {
	outerCtx

	ctx      sessionctx.Context
	executor Executor

	executorChk *chunk.Chunk

	maxBatchSize int
	batchSize    int

	taskCh  chan<- *indexJoinTask
	innerCh chan<- *indexJoinTask

	parentMemTracker *memory.Tracker
}

type innerWorker struct {
	innerCtx

	taskCh      <-chan *indexJoinTask
	outerCtx    outerCtx
	ctx         sessionctx.Context
	executorChk *chunk.Chunk

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
	joiner        joiner
	maxChunkSize  int

	workerId int
	closeCh  chan struct{}
}

type innerMergeWorker struct {
	innerWorker

	compareFuncs       []chunk.CompareFunc
	joinKeys           []*expression.Column
	curRowWithSameKeys []chunk.Row
	innerIter4Row      chunk.Iterator

	reader Executor

	sameKeyRows             []chunk.Row
	firstRow4Key            chunk.Row
	curNextRow              chunk.Row
	curInnerResult          *chunk.Chunk
	curIter                 *chunk.Iterator4Chunk
	curInnerResultInUse     bool
	resultQueue             []*chunk.Chunk
	resourceQueue           []*chunk.Chunk
	joinResultChkResourceCh chan *chunk.Chunk

	memTracker *memory.Tracker
}

type innerHashWorker struct {
	innerWorker
	innerPtrBytes     [][]byte
	joinChkResourceCh []chan *chunk.Chunk
	joinResultCh      chan *indexJoinWorkerResult
}

func (e *IndexMergeJoin) Open(ctx context.Context) error {
	err, innerCh, workerCtx := e.open(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	e.innerCtx.compareFuncs = make([]chunk.CompareFunc, 0, len(e.innerCtx.joinKeys))
	for i := range e.innerCtx.joinKeys {
		e.innerCtx.compareFuncs = append(e.innerCtx.compareFuncs, chunk.GetCompareFunc(e.innerCtx.joinKeys[i].RetType)) //给每一行添加比较大小的函数
	}

	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerWorker(innerCh, i, e.innerCtx.compareFuncs, e.innerCtx.joinKeys).run(workerCtx, e.workerWg) //inner worker
	}

	return nil
}

func (e *IndexHashJoin) Open(ctx context.Context) error {
	err, innerCh, workerCtx := e.open(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	e.closeCh = make(chan struct{})
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	e.joinResultCh = make(chan *indexJoinWorkerResult, concurrency+1)
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	for i := int(0); i < concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- e.newFirstChunk()
	}

	e.workerWg.Add(concurrency)
	for i := int(0); i < concurrency; i++ {
		workerId := i
		go util.WithRecovery(func() { e.newInnerWorker(innerCh, workerId).run(workerCtx, e.workerWg) }, e.finishInnerWorker)
	}
	go util.WithRecovery(e.waitInnerHashWorkersAndCloseResultChan, nil)
	return nil
}
func (e *IndexHashJoin) finishInnerWorker(r interface{}) {
	if r != nil {
		e.joinResultCh <- &indexJoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.workerWg.Done()
}
func (e *IndexHashJoin) waitInnerHashWorkersAndCloseResultChan() {
	e.workerWg.Wait()
	close(e.joinResultCh)

}

func (e *IndexJoin) open(ctx context.Context) (error, chan *indexJoinTask, context.Context) {
	e.innerCtx.readerBuilder.getStartTS()

	err := e.children[0].Open(ctx)
	if err != nil {
		return errors.Trace(err), nil, nil
	}
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	e.taskCh = make(chan *indexJoinTask, concurrency)
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *indexJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(e.taskCh, innerCh).run(workerCtx, e.workerWg)
	return nil, innerCh, workerCtx
}

func (e *IndexJoin) newOuterWorker(taskCh, innerCh chan *indexJoinTask) *outerWorker {
	ow := &outerWorker{
		outerCtx:         e.outerCtx,
		ctx:              e.ctx,
		executor:         e.children[0],
		executorChk:      chunk.NewChunkWithCapacity(e.outerCtx.rowTypes, e.maxChunkSize),
		taskCh:           taskCh,
		innerCh:          innerCh,
		batchSize:        32,
		maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
		parentMemTracker: e.memTracker,
	}
	return ow
}
func (iw *innerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	var task *indexJoinTask

	for {
		ok := true
		select {
		case task, ok = <-iw.taskCh:
			if !ok || task.buildError != nil {
				return
			}
		case <-ctx.Done():
			return
		}

		err := iw.handleTask(ctx, task)

		if err != nil {
			return
		}
	}
}

func (iw *innerHashWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	var task *indexJoinTask
	ok, joinResult := iw.getNewJoinResult()
	if !ok {
		return
	}
	for {
		select {
		case <-iw.closeCh:
			return
		case task, ok = <-iw.taskCh:
			if !ok || task.buildError != nil {
				return
			}
		case <-ctx.Done():
			return
		}
		err := iw.handleTask(ctx, task, joinResult)
		if err != nil {
			return
		}
	}
}

func (iw *innerMergeWorker) handleTask(ctx context.Context, task *indexJoinTask) error {
	dLookUpKeys, err := iw.constructDatumLookupKeys(task)
	if err != nil {
		ok, joinResult := iw.newIndexWorkerResult()

		if !ok {
			close(task.joinResultCh)
			return err
		}
		joinResult.err = err
		task.joinResultCh <- joinResult
		return err
	}
	dLookUpKeys = iw.sortAndDedupDatumLookUpKeys(dLookUpKeys)
	iw.reader, err = iw.readerBuilder.buildExecutorForIndexJoin(ctx, dLookUpKeys, iw.indexRanges, iw.keyOff2IdxOff)
	if err != nil {
		ok, joinResult := iw.newIndexWorkerResult()

		if !ok {
			close(task.joinResultCh)
			return err
		}

		joinResult.err = err
		task.joinResultCh <- joinResult

		return err
	}

	task.outIter = chunk.NewIterator4Chunk(task.outerResult)
	task.outerRow = task.outIter.Begin()

	iw.firstRow4Key, err = iw.nextRow(ctx)

	err = iw.joinToChunk(ctx, task)

	if err != nil {
		return err
	}

	return nil
}

func (iw *innerMergeWorker) joinToChunk(ctx context.Context, task *indexJoinTask) error {

	needNewJoinResult := true

	var joinResult *indexJoinWorkerResult
	var ok bool

	for {
		if needNewJoinResult {
			ok, joinResult = iw.newIndexWorkerResult()
			if !ok {
				close(task.joinResultCh)
				return errors.New("newIndexWorkerResult failed")
			}

			needNewJoinResult = false
		}

		if task.outerRow == task.outIter.End() {
			task.joinResultCh <- joinResult
			close(task.joinResultCh)
			return nil
		}

		if len(iw.curRowWithSameKeys) == 0 {
			if err := iw.fetchNextInnerRows(ctx); err != nil {
				joinResult.err = errors.Trace(err)
				task.joinResultCh <- joinResult
				close(task.joinResultCh)
				return nil
			}
		}

		cmpResult := -1
		if (task.outerMatch == nil || task.outerMatch[task.outerRow.Idx()]) && len(iw.curRowWithSameKeys) > 0 {
			cmpResult = compareChunkRow(iw.compareFuncs, task.outerRow, iw.curRowWithSameKeys[0], iw.outerCtx.joinKeys, iw.innerCtx.joinKeys)
		}

		if cmpResult > 0 {
			if err := iw.fetchNextInnerRows(ctx); err != nil {
				joinResult.err = errors.Trace(err)
				task.joinResultCh <- joinResult
				close(task.joinResultCh)
				return err
			}
			continue
		}

		if cmpResult < 0 {
			iw.joiner.onMissMatch(task.outerRow, joinResult.chk)

			task.outerRow = task.outIter.Next()
			task.hasMatch = false

			if joinResult.chk.NumRows() >= iw.maxChunkSize {
				task.joinResultCh <- joinResult
				needNewJoinResult = true
			}
			continue
		}

		matched, err := iw.joiner.tryToMatch(task.outerRow, iw.innerIter4Row, joinResult.chk)
		if err != nil {
			joinResult.err = err
			task.joinResultCh <- joinResult
			close(task.joinResultCh)
		}
		task.hasMatch = task.hasMatch || matched

		if iw.innerIter4Row.Current() == iw.innerIter4Row.End() {
			if !task.hasMatch {
				iw.joiner.onMissMatch(task.outerRow, joinResult.chk)
			}
			task.outerRow = task.outIter.Next()
			iw.innerIter4Row.Begin()
		}

		if joinResult.chk.NumRows() >= joinResult.chk.Capacity() {
			task.joinResultCh <- joinResult
			needNewJoinResult = true
		}
	}
}

func (e *innerMergeWorker) fetchNextInnerRows(ctx context.Context) (err error) {
	e.curRowWithSameKeys, err = e.rowsWithSameKey(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.innerIter4Row = chunk.NewIterator4Slice(e.curRowWithSameKeys)
	e.innerIter4Row.Begin()
	return nil
}

func (t *innerMergeWorker) rowsWithSameKey(ctx context.Context) ([]chunk.Row, error) {
	lastResultIdx := len(t.resultQueue) - 1
	t.resourceQueue = append(t.resourceQueue, t.resultQueue[0:lastResultIdx]...)
	t.resultQueue = t.resultQueue[lastResultIdx:]
	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		return nil, nil
	}
	t.sameKeyRows = t.sameKeyRows[:0]
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)
	for {
		selectedRow, err := t.nextRow(ctx)
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {
			t.firstRow4Key = t.curIter.End()
			return t.sameKeyRows, errors.Trace(err)
		}
		compareResult := compareChunkRow(t.compareFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			t.sameKeyRows = append(t.sameKeyRows, selectedRow)
		} else {
			t.firstRow4Key = selectedRow
			return t.sameKeyRows, nil
		}
	}
}

func (t *innerMergeWorker) nextRow(ctx context.Context) (chunk.Row, error) {
	for {
		if t.curNextRow == t.curIter.End() {
			t.reallocReaderResult()
			oldMemUsage := t.curInnerResult.MemoryUsage()
			err := t.reader.Next(ctx, t.curInnerResult)
			// error happens or no more data.
			if err != nil || t.curInnerResult.NumRows() == 0 {
				t.curNextRow = t.curIter.End()
				return t.curNextRow, errors.Trace(err)
			}
			newMemUsage := t.curInnerResult.MemoryUsage()
			t.memTracker.Consume(newMemUsage - oldMemUsage)
			t.curNextRow = t.curIter.Begin()
		}

		result := t.curNextRow
		t.curInnerResultInUse = true
		t.curNextRow = t.curIter.Next()

		if !t.hasNullInJoinKey(result) {
			return result, nil
		}
	}
}

func (t *innerMergeWorker) reallocReaderResult() {
	if !t.curInnerResultInUse {
		// If "t.curInnerResult" is not in use, we can just reuse it.
		t.curInnerResult.Reset()
		return
	}

	if len(t.resourceQueue) == 0 {
		newChunk := t.reader.newFirstChunk()
		t.memTracker.Consume(newChunk.MemoryUsage())
		t.resourceQueue = append(t.resourceQueue, newChunk)
	}

	// NOTE: "t.curResult" is always the last element of "resultQueue".
	t.curInnerResult = t.resourceQueue[0]
	t.curIter = chunk.NewIterator4Chunk(t.curInnerResult)
	t.resourceQueue = t.resourceQueue[1:]
	t.resultQueue = append(t.resultQueue, t.curInnerResult)
	t.curInnerResult.Reset()
	t.curInnerResultInUse = false
}

func (iw *innerHashWorker) handleTask(ctx context.Context, task *indexJoinTask, joinResult *indexJoinWorkerResult) error {

	dLookUpKeys, err := iw.constructDatumLookupKeys(task)
	if err != nil {
		return errors.Trace(err)
	}
	dLookUpKeys = iw.sortAndDedupDatumLookUpKeys(dLookUpKeys)
	err = iw.fetchAndJoin(ctx, task, dLookUpKeys, joinResult)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (iw *innerWorker) sortAndDedupDatumLookUpKeys(dLookUpKeys [][]types.Datum) [][]types.Datum {
	if len(dLookUpKeys) < 2 {
		return dLookUpKeys
	}
	sc := iw.ctx.GetSessionVars().StmtCtx
	sort.Slice(dLookUpKeys, func(i, j int) bool {
		cmp := compareRow(sc, dLookUpKeys[i], dLookUpKeys[j])
		return cmp < 0
	})
	deDupedLookupKeys := dLookUpKeys[:1]
	for i := 1; i < len(dLookUpKeys); i++ {
		cmp := compareRow(sc, dLookUpKeys[i], dLookUpKeys[i-1])
		if cmp != 0 {
			deDupedLookupKeys = append(deDupedLookupKeys, dLookUpKeys[i])
		}
	}
	return deDupedLookupKeys
}
func compareRow(sc *stmtctx.StatementContext, left, right []types.Datum) int {
	for idx := 0; idx < len(left); idx++ {
		cmp, err := left[idx].CompareDatum(sc, &right[idx])
		// We only compare rows with the same type, no error to return.
		terror.Log(err)
		if cmp > 0 {
			return 1
		} else if cmp < 0 {
			return -1
		}
	}
	return 0
}

func (iw *innerHashWorker) joinMatchInnerRow2Chunk(innerRow chunk.Row, task *indexJoinTask,
	joinResult *indexJoinWorkerResult) (bool, *indexJoinWorkerResult) {
	keyBuf := make([]byte, 0, 64)
	for _, keyCol := range iw.keyCols {
		d := innerRow.GetDatum(keyCol, iw.rowTypes[keyCol])
		var err error
		keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, d)
		if err != nil {
			return false, joinResult
		}
	}
	iw.innerPtrBytes = task.lookupMap.Get(keyBuf, iw.innerPtrBytes[:0])

	if len(iw.innerPtrBytes) == 0 {
		return true, joinResult
	}
	task.matchedOuters = task.matchedOuters[:0]
	for _, b := range iw.innerPtrBytes {
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedOuter := task.outerResult.GetRow(int(ptr.RowIdx))
		task.matchedOuters = append(task.matchedOuters, matchedOuter)
	}

	outerIter := chunk.NewIterator4Slice(task.matchedOuters)

	hasMatch := false
	for outerIter.Begin(); outerIter.Current() != outerIter.End(); {
		matched, err := iw.joiner.tryToMatch(innerRow, outerIter, joinResult.chk)
		if err != nil {
			joinResult.err = errors.Trace(err)
			return false, joinResult
		}
		hasMatch = hasMatch || matched
		if joinResult.chk.NumRows() == iw.maxChunkSize {
			ok := true
			iw.joinResultCh <- joinResult
			ok, joinResult = iw.getNewJoinResult()
			if !ok {
				return false, joinResult
			}
		}
	}
	if hasMatch {
		task.matchKeyMap.Put(keyBuf, []byte{0})
	}

	return true, joinResult

}

func (iw *innerHashWorker) join2Chunk(innerChk *chunk.Chunk, joinResult *indexJoinWorkerResult, task *indexJoinTask) (ok bool, _ *indexJoinWorkerResult) {
	for i := 0; i < innerChk.NumRows(); i++ {
		innerRow := innerChk.GetRow(i)

		ok, joinResult = iw.joinMatchInnerRow2Chunk(innerRow, task, joinResult)
		if !ok {
			return false, joinResult
		}
	}

	return true, joinResult

}
func (iw *innerWorker) hasNullInJoinKey(row chunk.Row) bool {
	for _, ordinal := range iw.outerCtx.keyCols {
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

func (iw *innerWorker) constructDatumLookupKeys(task *indexJoinTask) ([][]types.Datum, error) {
	dLookUpKeys := make([][]types.Datum, 0, task.outerResult.NumRows())
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	for i := 0; i < task.outerResult.NumRows(); i++ {
		dLookUpKey, err := iw.constructDatumLookupKey(task, i)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if dLookUpKey == nil {
			continue
		}
		keyBuf = keyBuf[:0]
		keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, dLookUpKey...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dLookUpKeys = append(dLookUpKeys, dLookUpKey)
		outerRow := task.outerResult.GetRow(i)

		if iw.hasNullInJoinKey(outerRow) { //skip outer row?
			continue
		}
		rowPtr := chunk.RowPtr{ChkIdx: uint32(0), RowIdx: uint32(i)}
		*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
		task.lookupMap.Put(keyBuf, valBuf)
	}
	return dLookUpKeys, nil
}

func (iw *innerWorker) constructDatumLookupKey(task *indexJoinTask, rowIdx int) ([]types.Datum, error) {
	if task.outerMatch != nil && !task.outerMatch[rowIdx] {
		return nil, nil
	}
	outerRow := task.outerResult.GetRow(rowIdx)
	sc := iw.ctx.GetSessionVars().StmtCtx
	keyLen := len(iw.keyCols)
	dLookupKey := make([]types.Datum, 0, keyLen)
	for i, keyCol := range iw.outerCtx.keyCols {
		outerValue := outerRow.GetDatum(keyCol, iw.outerCtx.rowTypes[keyCol])

		innerColType := iw.rowTypes[iw.keyCols[i]]
		innerValue, err := outerValue.ConvertTo(sc, innerColType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cmp, err := outerValue.CompareDatum(sc, &innerValue)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cmp != 0 {
			// If the converted outerValue is not equal to the origin outerValue, we don't need to lookup it.
			return nil, nil
		}
		dLookupKey = append(dLookupKey, innerValue)
	}
	return dLookupKey, nil
}

func (iw *innerMergeWorker) newIndexWorkerResult() (bool, *indexJoinWorkerResult) {
	joinResult := &indexJoinWorkerResult{
		src: iw.joinResultChkResourceCh,
	}
	ok := true
	select {
	case <-iw.closeCh:
		ok = false
	case joinResult.chk, ok = <-iw.joinResultChkResourceCh:
	}
	return ok, joinResult
}

func (iw *innerHashWorker) getNewJoinResult() (bool, *indexJoinWorkerResult) {
	joinResult := &indexJoinWorkerResult{
		src: iw.joinChkResourceCh[iw.workerId],
	}
	ok := true
	select {
	case <-iw.closeCh:
		ok = false
	case joinResult.chk, ok = <-iw.joinChkResourceCh[iw.workerId]:
	}
	return ok, joinResult
}

func (iw *innerHashWorker) fetchAndJoin(ctx context.Context, task *indexJoinTask, dLookUpKeys [][]types.Datum, joinResult *indexJoinWorkerResult) error {
	innerExec, err := iw.readerBuilder.buildExecutorForIndexJoin(ctx, dLookUpKeys, iw.indexRanges, iw.keyOff2IdxOff)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(innerExec.Close)
	innerResult := chunk.NewList(innerExec.retTypes(), iw.ctx.GetSessionVars().MaxChunkSize, iw.ctx.GetSessionVars().MaxChunkSize)
	innerResult.GetMemTracker().SetLabel("inner result")
	innerResult.GetMemTracker().AttachTo(task.memTracker)
	iw.executorChk.Reset()
	var ok bool
	for {
		err := innerExec.Next(ctx, iw.executorChk)
		if err != nil {
			return errors.Trace(err)
		}

		if iw.executorChk.NumRows() == 0 {
			break
		}

		ok, joinResult = iw.join2Chunk(iw.executorChk, joinResult, task)
		if !ok {
			break
		}
	}

	it := task.lookupMap.NewIterator()
	for i := 0; i < task.outerResult.NumRows(); i++ {
		key, rowPtr := it.Next()
		if key == nil || rowPtr == nil {
			break
		}
		iw.innerPtrBytes = task.matchKeyMap.Get(key, iw.innerPtrBytes[:0])
		if len(iw.innerPtrBytes) == 0 {
			ptr := *(*chunk.RowPtr)(unsafe.Pointer(&rowPtr[0]))
			misMatchedRow := task.outerResult.GetRow(int(ptr.RowIdx))
			iw.joiner.onMissMatch(misMatchedRow, joinResult.chk)
		}
	}

	if joinResult == nil {
		return nil
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		iw.joinResultCh <- joinResult
	}
	if !ok {
		return errors.New("join2Chunk failed")
	}
	return nil
}
func (e *IndexJoin) newBaseInnerWorker(taskCh chan *indexJoinTask, workerId int, closeCh chan struct{}) *innerWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}

	iw := &innerWorker{
		innerCtx:      e.innerCtx,
		outerCtx:      e.outerCtx,
		taskCh:        taskCh,
		ctx:           e.ctx,
		executorChk:   chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
		indexRanges:   copiedRanges,
		keyOff2IdxOff: e.keyOff2IdxOff,
		joiner:        e.joiner,
		maxChunkSize:  e.maxChunkSize,
		workerId:      workerId,
		closeCh:       closeCh,
	}
	return iw
}
func (e *IndexMergeJoin) newInnerWorker(innerTaskCh chan *indexJoinTask, workerId int, compareFuncs []chunk.CompareFunc, joinKeys []*expression.Column) *innerMergeWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	bw := e.newBaseInnerWorker(innerTaskCh, workerId, e.closeCh)

	resultQueue := make([]*chunk.Chunk, 0)
	resultQueue = append(resultQueue, chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize))

	innerResult := chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize)
	curIter := chunk.NewIterator4Chunk(innerResult)
	joinChkResourceCh := make(chan *chunk.Chunk, 1)
	joinChkResourceCh <- e.newFirstChunk()
	iw := &innerMergeWorker{
		innerWorker:             *bw,
		compareFuncs:            compareFuncs,
		joinKeys:                joinKeys,
		resultQueue:             resultQueue,
		curInnerResult:          innerResult,
		curIter:                 curIter,
		joinResultChkResourceCh: joinChkResourceCh,
	}

	return iw
}
func (e *IndexHashJoin) newInnerWorker(taskCh chan *indexJoinTask, workerId int) *innerHashWorker {

	bw := e.newBaseInnerWorker(taskCh, workerId, e.closeCh)

	iw := &innerHashWorker{
		innerWorker:       *bw,
		joinChkResourceCh: e.joinChkResourceCh,
		joinResultCh:      e.joinResultCh,
	}
	return iw
}

func (ow *outerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("outerWorker panic stack is:\n%s", buf)
		}
		if ow.keepOrder {
			close(ow.taskCh)
		}
		close(ow.innerCh)
		wg.Done()
	}()
	for {
		task, err := ow.buildTask(ctx)
		if task == nil {
			return
		}

		if ow.pushToAllChan(ctx, task) {
			return
		}

		if err != nil {
			break
		}
	}
}

// buildTask builds a indexJoinTask and read outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task.
func (ow *outerWorker) buildTask(ctx context.Context) (*indexJoinTask, error) {
	ow.executor.newFirstChunk()

	task := &indexJoinTask{
		outerResult:  ow.executor.newFirstChunk(),
		lookupMap:    mvmap.NewMVMap(),
		matchKeyMap:  mvmap.NewMVMap(),
		joinResultCh: make(chan *indexJoinWorkerResult),
	}
	task.memTracker = memory.NewTracker(fmt.Sprintf("lookup join task %p", task), -1)
	task.memTracker.AttachTo(ow.parentMemTracker)

	ow.increaseBatchSize()

	task.memTracker.Consume(task.outerResult.MemoryUsage())
	for task.outerResult.NumRows() < ow.batchSize {
		err := ow.executor.Next(ctx, ow.executorChk)
		if err != nil {
			task.buildError = err
			return task, errors.Trace(err)
		}
		if ow.executorChk.NumRows() == 0 {
			break
		}

		oldMemUsage := task.outerResult.MemoryUsage()
		task.outerResult.Append(ow.executorChk, 0, ow.executorChk.NumRows())
		newMemUsage := task.outerResult.MemoryUsage()
		task.memTracker.Consume(newMemUsage - oldMemUsage)
	}
	if task.outerResult.NumRows() == 0 {
		return nil, nil
	}

	if ow.filter != nil {
		outerMatch := make([]bool, 0, task.outerResult.NumRows())
		var err error
		task.outerMatch, err = expression.VectorizedFilter(ow.ctx, ow.filter, chunk.NewIterator4Chunk(task.outerResult), outerMatch)
		if err != nil {
			task.buildError = err
			return task, errors.Trace(err)
		}
		task.memTracker.Consume(int64(cap(task.outerMatch)))
	}
	return task, nil
}

func (ow *outerWorker) increaseBatchSize() {
	if ow.batchSize < ow.maxBatchSize {
		ow.batchSize *= 2
	}
	if ow.batchSize > ow.maxBatchSize {
		ow.batchSize = ow.maxBatchSize
	}
}

func (ow *outerWorker) pushToAllChan(ctx context.Context, task *indexJoinTask) bool {
	innerFinished := ow.pushToChan(ctx, task, ow.innerCh)

	if ow.outerCtx.keepOrder {
		keepOrderFinished := ow.pushToChan(ctx, task, ow.taskCh)

		return innerFinished || keepOrderFinished
	}

	return innerFinished
}

func (ow *outerWorker) pushToChan(ctx context.Context, task *indexJoinTask, dst chan<- *indexJoinTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

func (e *IndexHashJoin) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.joinResultCh == nil {
		return nil
	}
	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}
	chk.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *IndexMergeJoin) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	joinResult := e.getJoinResult(ctx)

	if joinResult == nil {
		return nil
	}

	if joinResult.err != nil {
		return errors.Trace(joinResult.err)
	}

	chk.SwapColumns(joinResult.chk)
	joinResult.src <- joinResult.chk
	return nil
}

func (e *IndexMergeJoin) getJoinResult(ctx context.Context) *indexJoinWorkerResult {
	for {
		joinResultCh := e.getJoinResultCh(ctx)

		if joinResultCh != nil {
			joinResult, ok := <-joinResultCh
			if ok {
				return joinResult
			} else {
				e.joinResultCh = nil //clear e.joinResultCh
				continue
			}
		} else {
			return nil
		}
	}
}

func (e *IndexMergeJoin) getJoinResultCh(ctx context.Context) chan *indexJoinWorkerResult {
	joinResultCh := e.joinResultCh

	if joinResultCh != nil {
		return joinResultCh
	}

	e.joinResultCh = e.getNextJoinResultCh(ctx)

	return e.joinResultCh
}

func (e *IndexMergeJoin) getNextJoinResultCh(ctx context.Context) chan *indexJoinWorkerResult {
	var task *indexJoinTask
	ok := true

	select {
	case task, ok = <-e.taskCh:
	case <-ctx.Done():
		return nil
	}
	if ok && task.buildError == nil {
		return task.joinResultCh
	} else {
		return nil
	}
}

func (e *IndexHashJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	close(e.closeCh)
	if e.joinResultCh != nil {
		for range e.joinResultCh {
		}
	}
	for i := range e.joinChkResourceCh {
		close(e.joinChkResourceCh[i])
		for range e.joinChkResourceCh[i] {
		}
	}
	e.joinChkResourceCh = nil

	e.memTracker.Detach()
	e.memTracker = nil
	return errors.Trace(e.children[0].Close())
}
func (e *IndexMergeJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}

	e.memTracker.Detach()
	e.memTracker = nil
	return errors.Trace(e.children[0].Close())
}
