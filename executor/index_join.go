package executor

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
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

	task     *indexJoinTask
	taskCh chan *indexJoinTask

	joinResult *chunk.Chunk
	innerIter  chunk.Iterator

	joiner joiner

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int

	memTracker *memory.Tracker // track memory usage.
	prepare    bool
	workerCtx  context.Context

	closeCh           chan struct{} // closeCh add a lock for closing executor.
}
type outerCtx struct {
	rowTypes  []*types.FieldType
	keyCols   []int
	joinKeys []*expression.Column	//哪些列是用来比较的

	filter    expression.CNFExprs
	keepOrder bool
}

type innerCtx struct {
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	keyCols       []int
	joinKeys []*expression.Column	//哪些列是用来比较的

	compareFuncs   []chunk.CompareFunc
}

type indexJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool


	outIter       *chunk.Iterator4Chunk //在inner handle task的时候进行初始化
	outerRow      chunk.Row             //同上

	encodedLookUpKeys *chunk.Chunk
	lookupMap     *mvmap.MVMap
	matchKeyMap   *mvmap.MVMap
	matchedOuters []chunk.Row


	doneCh   chan error	//这个的存在没有什么必要，没有人读，只有人写
	cursor   int
	hasMatch bool

	memTracker *memory.Tracker // track memory usage.

	//merge join use
	joinResultCh      chan *indexJoinWorkerResult

}

type indexMergeJoinTask struct {
	indexJoinTask
	joinResultCh      chan *indexJoinWorkerResult
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

	taskCh chan<- *indexJoinTask
	innerCh  chan<- *indexJoinTask

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
	closeCh       chan struct{}
}

type innerMergeWorker struct {
	innerWorker

	compareFuncs       []chunk.CompareFunc
	joinKeys           []*expression.Column //哪些列是用来比较的
	curRowWithSameKeys []chunk.Row          //inner的相同列的row
	innerIter4Row      chunk.Iterator       //innerRow的it

	reader   Executor

	// for chunk executions
	sameKeyRows              []chunk.Row
	firstRow4Key             chunk.Row				//等于说是把inner按照sameKey来分段，这个是第一段的第一行		其实应该赋值给task
	//	innerReaderResourceChunk *chunk.Chunk
	curNextRow               chunk.Row //这个是第二行数据，执行的过程中firstRow4Key的值就是curRow，而curRow则会成为curIter.next
	curInnerResult           *chunk.Chunk
	curIter                  *chunk.Iterator4Chunk //是curResult的一个迭代器(最后一行数据是一个空行)
	curInnerResultInUse      bool                  //当前curResult是否正在使用
	resultQueue    []*chunk.Chunk														//chunk属于资源型的，使用前应该reset掉
	resourceQueue  []*chunk.Chunk			//这个可以理解为是一个资源队列					//同上
	joinResultChkResourceCh chan *chunk.Chunk //这个是join结果的资源队列，用来和next交换用的

	memTracker *memory.Tracker
}

type innerHashWorker struct {
	innerWorker
	innerPtrBytes     [][]byte
//	workerId          int
	joinChkResourceCh []chan *chunk.Chunk
//	closeCh           chan struct{}
	joinResultCh      chan *indexJoinWorkerResult
}

func (e *IndexMergeJoin) Open(ctx context.Context) error {
	log.Info("use index merge join")
	err, innerCh, workerCtx := e.open(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	e.innerCtx.compareFuncs = make([]chunk.CompareFunc, 0, len(e.innerCtx.joinKeys))
	log.Info("innerCtx joinKeys", e.innerCtx.joinKeys)
	for i := range e.innerCtx.joinKeys {
		e.innerCtx.compareFuncs = append(e.innerCtx.compareFuncs, chunk.GetCompareFunc(e.innerCtx.joinKeys[i].RetType))//给每一行添加比较大小的函数
	}

	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerWorker(innerCh , i , e.innerCtx.compareFuncs, e.innerCtx.joinKeys).run(workerCtx, e.workerWg)	//inner worker
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
	defer func(){
		wg.Done()
	}()

	log.Info("innerMergeWorker in, workerId:" , iw.workerId)
	var task *indexJoinTask

	for ok := true; ok; {
		select {
		case task, ok = <-iw.taskCh:
			if !ok {
				log.Info("innerMergeWorker innerTaskCh close so exit, workerId:" , iw.workerId)
				return
			}
		case <-ctx.Done():
			log.Info("innerMergeWorker ctx done so exit, workerId:" , iw.workerId)
			return
		}

		log.Info("innerMergeWorker begin process task, workerId:" , iw.workerId)
		iw.handleTask(ctx, task)
		log.Info("innerMergeWorker end process task, workerId:" , iw.workerId)
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
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}
		err := iw.handleTask(ctx, task, joinResult)
		if err != nil {
			return	//error 未处理
		}
	}
}

func (iw *innerMergeWorker) handleTask(ctx context.Context, task *indexJoinTask) {
	dLookUpKeys, err := iw.constructDatumLookupKeys(task)
	if err != nil {
		ok, joinResult := iw.newIndexWorkerResult()

		if !ok {
			close(task.joinResultCh)
			return
		}

		joinResult.err = err
		task.joinResultCh <- joinResult

		return
	}
	dLookUpKeys = iw.sortAndDedupDatumLookUpKeys(dLookUpKeys)

	iw.reader, err = iw.readerBuilder.buildExecutorForIndexJoin(ctx, dLookUpKeys, iw.indexRanges, iw.keyOff2IdxOff)//构建了一个exec来获取数据	todo 这个地方应该把joinToChunk加到innerExec的轮训里面来，不应该搞到iw里面
	if err != nil {
		ok, joinResult := iw.newIndexWorkerResult()

		if !ok {
			close(task.joinResultCh)
			return
		}

		joinResult.err = err
		task.joinResultCh <- joinResult

		return
	}

	task.outIter = chunk.NewIterator4Chunk(task.outerResult)
	task.outerRow = task.outIter.Begin()

	iw.firstRow4Key, err = iw.nextRow(ctx)	//初始化firstRow4Key为返回的第一行数据

	//然后和outer比较
	iw.joinToChunk(ctx, task)
}

func (iw *innerMergeWorker) joinToChunk(ctx context.Context, task *indexJoinTask) {
	defer func() {
		log.Info("joinToChunk exit, task status")
	}()
	needNewJoinResult := true

	var joinResult *indexJoinWorkerResult
	var ok bool

	for {
		if needNewJoinResult {
			ok, joinResult = iw.newIndexWorkerResult()
			if !ok {
				close(task.joinResultCh)
				return
			}

			needNewJoinResult = false
		}

		if task.outerRow == task.outIter.End() {
			task.joinResultCh <- joinResult
			close(task.joinResultCh)
			return
		}

		if len(iw.curRowWithSameKeys) == 0 {
			if err := iw.fetchNextInnerRows(ctx); err != nil {
				joinResult.err = errors.Trace(err)
				task.joinResultCh <- joinResult
				close(task.joinResultCh)
				return
			}
		}

		cmpResult := -1
		if (task.outerMatch == nil || task.outerMatch[task.outerRow.Idx()]) && len(iw.curRowWithSameKeys) > 0 {
			log.Info("compareChunkRow")
			cmpResult = compareChunkRow(iw.compareFuncs, task.outerRow, iw.curRowWithSameKeys[0], iw.outerCtx.joinKeys, iw.innerCtx.joinKeys)
		}

		if cmpResult > 0 {//outer比inner大，匹配不上，那么拉取inner的下一行
			log.Info("cmpResult > 0")
			if err := iw.fetchNextInnerRows(ctx); err != nil {
				joinResult.err = errors.Trace(err)
				task.joinResultCh <- joinResult
				close(task.joinResultCh)
				return
			}
			continue
		}

		if cmpResult < 0 {//inner比outer大，匹配不上，那么处理missMatch的情况，并更新outer为下一行
			log.Info("cmpResult < 0")
			iw.joiner.onMissMatch(task.outerRow, joinResult.chk)

			task.outerRow = task.outIter.Next()
			task.hasMatch = false

			if joinResult.chk.NumRows() >= iw.maxChunkSize {
				task.joinResultCh <- joinResult
				needNewJoinResult = true
			}
			continue
		}

		log.Info("cmpResult equals 0")
		log.Info("joinResult.chk col nums" , joinResult.chk.NumCols())
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
	e.curRowWithSameKeys, err = e.rowsWithSameKey(ctx) //把相同的key的行捞出来了
	if err != nil {
		return errors.Trace(err)
	}
	e.innerIter4Row = chunk.NewIterator4Slice(e.curRowWithSameKeys)
	e.innerIter4Row.Begin()
	return nil
}

func (t *innerMergeWorker) rowsWithSameKey(ctx context.Context) ([]chunk.Row, error) {
	lastResultIdx := len(t.resultQueue) - 1
	log.Info("lastResultIdx:" , lastResultIdx)
	t.resourceQueue = append(t.resourceQueue, t.resultQueue[0:lastResultIdx]...)//看样子应该是回收资源来的️
	t.resultQueue = t.resultQueue[lastResultIdx:]	//只保留了最后一个元素(看前面代码是curResult)
	// no more data.
	if t.firstRow4Key == t.curIter.End() {//说明没有更新的数据了
		return nil, nil
	}
	t.sameKeyRows = t.sameKeyRows[:0] //清空了sameKeyRows
	t.sameKeyRows = append(t.sameKeyRows, t.firstRow4Key)//把第一行数据append到sameKeyRows里面了
	for {
		selectedRow, err := t.nextRow(ctx)	//获取下一行数据
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {//说明没有数据了，那么
			t.firstRow4Key = t.curIter.End()
			return t.sameKeyRows, errors.Trace(err)
		}
		compareResult := compareChunkRow(t.compareFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {//说明和firstRow4key相同，那么append
			t.sameKeyRows = append(t.sameKeyRows, selectedRow)
		} else {
			t.firstRow4Key = selectedRow	//不相等了，说明相同段的数据行已经都铣刀sameKeyRows里面了
			return t.sameKeyRows, nil
		}
	}
}

func (t *innerMergeWorker) nextRow(ctx context.Context) (chunk.Row, error) {
	for {
		if t.curNextRow == t.curIter.End() { //说明上一个迭代完成了，curRow就是空行
			t.reallocReaderResult()	//重新分配一个curResult出来
			oldMemUsage := t.curInnerResult.MemoryUsage()
			err := t.reader.Next(ctx, t.curInnerResult) //这个是写入curResult数据
			// error happens or no more data.
			if err != nil || t.curInnerResult.NumRows() == 0 { //说明没有数据了
				t.curNextRow = t.curIter.End()
				return t.curNextRow, errors.Trace(err)
			}
			newMemUsage := t.curInnerResult.MemoryUsage()
			t.memTracker.Consume(newMemUsage - oldMemUsage)
			t.curNextRow = t.curIter.Begin() //初始化curRow
		}

		result := t.curNextRow //把currentRow赋值给了result
		t.curInnerResultInUse = true
		t.curNextRow = t.curIter.Next() //当前行为curIter的next

		if !t.hasNullInJoinKey(result) {
			return result, nil	//返回这一行数据
		}
	}
}

func (t *innerMergeWorker) reallocReaderResult() {
	if !t.curInnerResultInUse {
		// If "t.curInnerResult" is not in use, we can just reuse it.  置空
		t.curInnerResult.Reset()
		return
	}

	// Create a new Chunk and append it to "resourceQueue" if there is no more  没有可用资源了，就需要创建一个
	// available chunk in "resourceQueue".
	//if t.innerReaderResourceChunk == nil {
	//	newChunk := t.reader.newFirstChunk()
	//	t.memTracker.Consume(newChunk.MemoryUsage())
	//	t.innerReaderResourceChunk = newChunk
	//}

	if len(t.resourceQueue) == 0 {
		newChunk := t.reader.newFirstChunk()
		t.memTracker.Consume(newChunk.MemoryUsage())
		t.resourceQueue = append(t.resourceQueue, newChunk)
	}

	//t.curInnerResult.SwapColumns(t.innerReaderResourceChunk)
	//t.curInnerResult.Reset()                                //设置为未使用的状态
	//t.curInnerResultInUse = false

	//	t.curIter = chunk.NewIterator4Chunk(t.curInnerResult)   //重新初始化curIter

	// NOTE: "t.curResult" is always the last element of "resultQueue".  curResult始终是resoucreQueue的第一个元素
	t.curInnerResult = t.resourceQueue[0]
	t.curIter = chunk.NewIterator4Chunk(t.curInnerResult)	//重新初始化curIter
	t.resourceQueue = t.resourceQueue[1:]	//因为上面用掉了一个，所以这里就补一个数据出来
	t.resultQueue = append(t.resultQueue, t.curInnerResult)	//把curResult放到了resultQueue里面
	t.curInnerResult.Reset()	//设置为未使用的状态
	t.curInnerResultInUse = false
}

func (iw *innerHashWorker) handleTask(ctx context.Context, task *indexJoinTask, joinResult *indexJoinWorkerResult) error {

	dLookUpKeys, err := iw.constructDatumLookupKeys(task)
	if err != nil {
		return errors.Trace(err)
	}
	dLookUpKeys = iw.sortAndDedupDatumLookUpKeys(dLookUpKeys)
	//err = iw.buildHashTable(task)
	//if err != nil {
	//	return errors.Trace(err)
	//}
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

func (iw *innerWorker) buildHashTable(task *indexJoinTask) error {
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	for i := 0; i < task.outerResult.NumRows(); i++ {
		if task.outerMatch != nil && !task.outerMatch[i] {
			continue
		}
		outerRow := task.outerResult.GetRow(i)
		if iw.hasNullInJoinKey(outerRow) { //skip outer row?
			continue
		}

		//keyBuf = keyBuf[:0]
		//for _, keyCol := range ow.keyCols {
		//	d := outerRow.GetDatum(keyCol, ow.rowTypes[keyCol])
		//	var err error
		//	keyBuf, err = codec.EncodeKey(ow.ctx.GetSessionVars().StmtCtx, keyBuf, d)
		//	if err != nil {
		//		return errors.Trace(err)
		//	}
		//}
		keyBuf = task.encodedLookUpKeys.GetRow(i).GetBytes(0)
		log.Infof("outer Key %v", keyBuf)

		rowPtr := chunk.RowPtr{ChkIdx: uint32(0), RowIdx: uint32(i)}
		*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
		task.lookupMap.Put(keyBuf, valBuf)
	}
	return nil
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
	log.Infof("inner Key %v", keyBuf)
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
			// Append null to make looUpKeys the same length as outer Result.
			task.encodedLookUpKeys.AppendNull(0)
			continue
		}
		keyBuf = keyBuf[:0]
		keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, dLookUpKey...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		log.Infof("outer Key %v", keyBuf)
		// Store the encoded lookup key in chunk, so we can use it to lookup the matched inners directly.
		task.encodedLookUpKeys.AppendBytes(0, keyBuf)
		dLookUpKeys = append(dLookUpKeys, dLookUpKey)

		outerRow := task.outerResult.GetRow(i)

		if iw.hasNullInJoinKey(outerRow) { //skip outer row?
			continue
		}
		rowPtr := chunk.RowPtr{ChkIdx: uint32(0), RowIdx: uint32(i)}
		*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
		task.lookupMap.Put(keyBuf, valBuf)
	}

	//task.memTracker.Consume(task.encodedLookUpKeys.MemoryUsage())
	task.memTracker.Consume(task.encodedLookUpKeys.MemoryUsage())
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
	defer func() {
		log.Info("end new index worker result")
	}()
	log.Info("begin new index worker result")
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
		workerId:	   workerId,
		closeCh:	   closeCh,
	}
	return iw
}
func (e *IndexMergeJoin) newInnerWorker(innerTaskCh chan *indexJoinTask, workerId int, compareFuncs []chunk.CompareFunc, joinKeys []*expression.Column) *innerMergeWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	bw := e.newBaseInnerWorker(innerTaskCh, workerId, e.closeCh)

	resultQueue := make([]*chunk.Chunk,0)
	resultQueue = append(resultQueue , chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize))

	innerResult := chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize)
	curIter := chunk.NewIterator4Chunk(innerResult)
	joinChkResourceCh := make(chan *chunk.Chunk,1)
	log.Info("joinChkResourceCh chunk capacity size" , len(e.innerCtx.rowTypes))
	joinChkResourceCh <- e.newFirstChunk()
	iw := &innerMergeWorker{
		innerWorker:    *bw,
		compareFuncs:   compareFuncs,
		joinKeys:       joinKeys,
		resultQueue:    resultQueue,
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
//		workerId:          workerId,
//		closeCh:           e.closeCh,
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
			//ow.pushToChan(ctx, task, ow.resultCh)
		}
		if ow.keepOrder {
			close(ow.taskCh)
		}
		close(ow.innerCh)
		wg.Done()
	}()
	for {
		task, err := ow.buildTask(ctx)
		if err != nil {
			task.doneCh <- errors.Trace(err)	//出问题了，应该通知next和inner的，而这个done.ch没有人去读的，所以没有必要保留；
			//ow.pushToChan(ctx, task, ow.resultCh)
			return
		}
		if task == nil {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}

		if ow.outerCtx.keepOrder {
			if finished := ow.pushToChan(ctx, task, ow.taskCh); finished {
				return
			}
		}
	}
}

// buildTask builds a indexJoinTask and read outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task.
func (ow *outerWorker) buildTask(ctx context.Context) (*indexJoinTask, error) {
	ow.executor.newFirstChunk()

	task := &indexJoinTask{

		doneCh:      make(chan error, 1),
		outerResult: ow.executor.newFirstChunk(),
		encodedLookUpKeys: chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, ow.ctx.GetSessionVars().MaxChunkSize),
		lookupMap:   mvmap.NewMVMap(),
		matchKeyMap: mvmap.NewMVMap(),
		joinResultCh: make(chan *indexJoinWorkerResult),
	}
	task.memTracker = memory.NewTracker(fmt.Sprintf("lookup join task %p", task), -1)
	task.memTracker.AttachTo(ow.parentMemTracker)

	ow.increaseBatchSize()

	task.memTracker.Consume(task.outerResult.MemoryUsage())
	for task.outerResult.NumRows() < ow.batchSize {
		err := ow.executor.Next(ctx, ow.executorChk)
		if err != nil {
			return task, errors.Trace(err)
		}
		log.Infof("outer row num %v", ow.executorChk.NumRows())
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
	e.joinResult.Reset()
	for {
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
}

func (e *IndexMergeJoin) getJoinResult(ctx context.Context) (*indexJoinWorkerResult) {
	defer func() {
		log.Info("get finishedTask end")
	}()
	log.Info("get finishedTask begin")
	for{
		task := e.task
		ok := true

		log.Info("wait join result")
		if task != nil {//说明这个task还没有跑完
			joinResult, ok := <-task.joinResultCh
			if !ok {	//说明task完成了,那么获取下一个task
				e.task = nil	//清空当前的task
				continue
			}
			return joinResult
		}

		log.Info("wait task")
		select {
		case task, ok = <-e.taskCh:
		case <-ctx.Done():
			return nil
		}
		if task == nil && !ok {	//outer已经关闭了，并且所有的task都运行完成了
			return nil
		}

		if e.task != nil {
			e.task.memTracker.Detach()
		}
		e.task = task
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
