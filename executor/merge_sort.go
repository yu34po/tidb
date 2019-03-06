// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"container/heap"
	"context"
	"github.com/cznic/mathutil"
	"sort"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
)

// MergeSortExec represents sorting executor.
type MergeSortExec struct {
	baseExecutor

	ByItems []*plannercore.ByItems
	fetched bool
	schema  *expression.Schema

	keyExprs []expression.Expression
	keyTypes []*types.FieldType
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// rowChunks is the chunks to store row values.
	rowChunks *chunk.List
	// rowPointer store the chunk index and row index for each row.
	workerRowPtrs []*[]chunk.RowPtr

	workerRowLen []uint64
	workerRowIdx []int

	memTracker  *memory.Tracker
	concurrency int
}

// sortWorker represents worker routine to process sort.
type sortWorker struct {
	MergeSortExec
	chkIdx  int
	rowIdx  int
	len     uint64
	rowPtrs []chunk.RowPtr
}

func (sw *sortWorker) run() {
	//sw.memTracker.Consume(int64(8 * sw.rowChunks.Len()))
	for chkIdx := sw.chkIdx; chkIdx < sw.rowChunks.NumChunks() && uint64(len(sw.rowPtrs)) < sw.len; chkIdx++ {
		rowChk := sw.rowChunks.GetChunk(chkIdx)
		rowIdx := 0
		if chkIdx == sw.chkIdx {
			rowIdx = sw.rowIdx
		}
		for ; rowIdx < rowChk.NumRows() && uint64(len(sw.rowPtrs)) < sw.len; rowIdx++ {
			sw.rowPtrs = append(sw.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
	sort.Slice(sw.rowPtrs, sw.keyColumnsLess)

	return
}

// Close implements the Executor Close interface.
func (e *MergeSortExec) Close() error {
	e.memTracker.Detach()
	e.memTracker = nil
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *MergeSortExec) Open(ctx context.Context) error{
	e.fetched = false
	e.concurrency = e.ctx.GetSessionVars().MergeSortConcurrency
	//e.concurrency = 1
	e.workerRowIdx = make([]int, e.concurrency)
	e.workerRowLen = make([]uint64, e.concurrency)
	e.workerRowPtrs = make([]*[]chunk.RowPtr, e.concurrency)
	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaSort)
		e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	}
	return errors.Trace(e.children[0].Open(ctx))
}

func (e *MergeSortExec) newsortWorker(workerID, chk, row int, len uint64) *sortWorker {
	sw := &sortWorker{
		MergeSortExec: *e,
		chkIdx:        chk,
		rowIdx:        row,
		len:           len,
		rowPtrs:       make([]chunk.RowPtr, 0, len),
	}
	e.workerRowLen[workerID] = len
	e.workerRowIdx[workerID] = 0
	e.workerRowPtrs[workerID] = &sw.rowPtrs
	return sw
}

//sortWorkerIndex calc the chunk index and row index with every worker start to sort, first column of swIdx is chunk idx, second columm of swIdx is row idx
func (e *MergeSortExec) sortWorkerIndex(workerRowsCount int) [][]int {
	chkIdx := 0
	rowIdx := 0
	swIdx := make([][]int, e.concurrency)
	swIdx[0] = []int{0, 0}
	for i := 1; i < e.concurrency; i++ {
		count := 0
		swIdx[i] = []int{0, 0}
		for j := chkIdx; j < e.rowChunks.NumChunks(); j++ {
			curChk := e.rowChunks.GetChunk(j)
			count += curChk.NumRows()
			if j == chkIdx {
				count -= rowIdx
			}
			if count > workerRowsCount {
				rowIdx = curChk.NumRows() - (count - workerRowsCount)
				chkIdx = j
				swIdx[i] = []int{chkIdx, rowIdx}
				break
			}
		}
	}
	return swIdx
}

// Next implements the Executor Next interface.
func (e *MergeSortExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("sort.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	req.Reset()
	if !e.fetched {
		err := e.fetchRowChunks(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		e.initCompareFuncs()
		e.buildKeyColumns()

		workerRowsCount := e.rowChunks.Len() / e.concurrency
		workerIdx := e.sortWorkerIndex(workerRowsCount)

		wg := &sync.WaitGroup{}
		wg.Add(int(e.concurrency))

		for i := 0; i < e.concurrency; i++ {
			// Last worker must complete the rest of rows.
			if i == e.concurrency-1 {
				workerRowsCount += e.rowChunks.Len() % e.concurrency
			}
			sw := e.newsortWorker(i, workerIdx[i][0], workerIdx[i][1], uint64(workerRowsCount))
			go util.WithRecovery(func() {
				defer wg.Done()
				sw.run()
			}, nil)
		}

		wg.Wait()
		e.fetched = true
	}

	for !req.IsFull() {
		j := 0
		for j < e.concurrency && uint64(e.workerRowIdx[j]) >= e.workerRowLen[j] {
			j++
		}
		if j >= e.concurrency {
			break
		}
		minRowPtr := (*e.workerRowPtrs[j])[e.workerRowIdx[j]]
		for i := j + 1; i < e.concurrency; i++ {
			if uint64(e.workerRowIdx[i]) < e.workerRowLen[i] {
				flag := false
				keyRowI := e.rowChunks.GetRow(minRowPtr)
				keyRowJ := e.rowChunks.GetRow((*e.workerRowPtrs[i])[e.workerRowIdx[i]])
				flag = e.lessRow(keyRowI, keyRowJ)
				if !flag {
					minRowPtr = (*e.workerRowPtrs[i])[e.workerRowIdx[i]]
					j = i
				}
			}
		}
		e.workerRowIdx[j]++
		req.AppendRow(e.rowChunks.GetRow(minRowPtr))
	}
	return nil
}

func (e *MergeSortExec) fetchRowChunks(ctx context.Context) error {
	fields := e.retTypes()
	e.rowChunks = chunk.NewList(fields, e.initCap, e.maxChunkSize)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel("rowChunks")
	for {
		chk := e.children[0].newFirstChunk()
		err := e.children[0].Next(ctx, chunk.NewRecordBatch(chk))
		if err != nil {
			return errors.Trace(err)
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		e.rowChunks.Add(chk)
	}
	return nil
}

func (e *MergeSortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *MergeSortExec) buildKeyColumns() {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		col := by.Expr.(*expression.Column)
		e.keyColumns = append(e.keyColumns, col.Index)
	}
}

func (e *MergeSortExec) buildKeyExprsAndTypes() {
	keyLen := len(e.ByItems)
	e.keyTypes = make([]*types.FieldType, keyLen)
	e.keyExprs = make([]expression.Expression, keyLen)
	for keyColIdx := range e.ByItems {
		e.keyExprs[keyColIdx] = e.ByItems[keyColIdx].Expr
		e.keyTypes[keyColIdx] = e.ByItems[keyColIdx].Expr.GetType()
	}
}

func (e *MergeSortExec) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

// keyColumnsLess is the less function for key columns.
func (sw *sortWorker) keyColumnsLess(i, j int) bool {
	rowI := sw.rowChunks.GetRow(sw.rowPtrs[i])
	rowJ := sw.rowChunks.GetRow(sw.rowPtrs[j])
	return sw.lessRow(rowI, rowJ)
}

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	MergeSortExec
	limit      *plannercore.PhysicalLimit
	totalLimit uint64

	chkHeap *topNChunkHeap
	idx     int
	offsetIdx int
	rowPtrs []chunk.RowPtr
	workerChunks []*chunk.List
}

// topNChunkHeap implements heap.Interface.
type topNChunkHeap struct {
	*topNWorker
}

type topNWorker struct {
	MergeSortExec
	totalLimit uint64

	chkHeap *topNChunkHeap
	rowPtrs []chunk.RowPtr

	rowChunks  *chunk.List
	rowChunkCh <-chan *chunk.Chunk
}

func (e *TopNExec)newTopNWorker(workerID int, chkList *chunk.List, chunkCh chan *chunk.Chunk, total uint64) *topNWorker {
	t := &topNWorker{
		MergeSortExec:e.MergeSortExec,
		rowChunks:  chkList,
		rowChunkCh: chunkCh,
		totalLimit: total,
	}

	e.workerChunks[workerID] = t.rowChunks
	e.workerRowIdx[workerID] = 0
	e.workerRowPtrs[workerID] = &t.rowPtrs
	e.workerRowLen[workerID] = uint64(mathutil.MinUint64(uint64(e.totalLimit), uint64(chkList.Len())))
	t.initPointers()
	t.initCompareFuncs()
	t.buildKeyColumns()
	//log.Infof("worker %d rowPtrsLen %d len %d %v", workerID, len(*e.workerRowPtrs[workerID]), e.workerRowLen[workerID], e.workerRowPtrs[workerID])

	return t
}

// keyColumnsLess is the less function for key columns.
func (t *topNWorker) keyColumnsLess(i, j int) bool {
	rowI := t.rowChunks.GetRow(t.rowPtrs[i])
	rowJ := t.rowChunks.GetRow(t.rowPtrs[j])
	return t.lessRow(rowI, rowJ)
}

func (t *topNWorker) run(ctx context.Context) {
	var (
		chunk *chunk.Chunk
		ok    bool
	)
	if t.rowChunks.Len() == 0 {
		return
	}
	t.chkHeap = &topNChunkHeap{t}
	heap.Init(t.chkHeap)
	for uint64(len(t.rowPtrs)) > t.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(t.chkHeap)
	}
	for {
		select {
		case chunk, ok = <-t.rowChunkCh:
			if !ok {
				//log.Infof("rowPtrs length %d", len(t.rowPtrs))
				sort.Slice(t.rowPtrs, t.keyColumnsLess)
				return
			}
		case <-ctx.Done():
			return
		}
		if chunk.NumRows() == 0 {
			break
		}
		err := t.processChildChk(chunk)
		if err != nil {
			return
		}
	}
}

// Less implement heap.Interface, but since we mantains a max heap,
// this function returns true if row i is greater than row j.
func (h *topNChunkHeap) Less(i, j int) bool {
	rowI := h.rowChunks.GetRow(h.rowPtrs[i])
	rowJ := h.rowChunks.GetRow(h.rowPtrs[j])
	return h.greaterRow(rowI, rowJ)
}

func (h *topNChunkHeap) greaterRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range h.keyColumns {
		cmpFunc := h.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if h.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp > 0 {
			return true
		} else if cmp < 0 {
			return false
		}
	}
	return false
}

func (h *topNChunkHeap) Len() int {
	return len(h.rowPtrs)
}

func (h *topNChunkHeap) Push(x interface{}) {
	// Should never be called.
}

func (h *topNChunkHeap) Pop() interface{} {
	h.rowPtrs = h.rowPtrs[:len(h.rowPtrs)-1]
	// We don't need the popped value, return nil to avoid memory allocation.
	return nil
}

func (h *topNChunkHeap) Swap(i, j int) {
	if j < 0 {
		return
	}

	h.rowPtrs[i], h.rowPtrs[j] = h.rowPtrs[j], h.rowPtrs[i]
}

// Open implements the Executor Open interface.
func (e *TopNExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaTopn)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	return errors.Trace(e.MergeSortExec.Open(ctx))
}

// Next implements the Executor Next interface.
func (e *TopNExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("topN.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	req.Reset()
	if !e.fetched {
		e.workerChunks = make([]*chunk.List, e.concurrency)
		e.totalLimit = e.limit.Offset + e.limit.Count

		e.idx = 0
		chkCh := make(chan *chunk.Chunk, e.concurrency)
		wg := &sync.WaitGroup{}
		wg.Add(int(e.concurrency))

		for i := 0; i < e.concurrency; i++ {
			chkList, err := e.loadChunksUntilTotalLimit(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			//log.Infof("total limit %d load chunks len %d", e.totalLimit, chkList.Len())
			tw := e.newTopNWorker(i, chkList, chkCh, e.totalLimit)
			e.keyColumns = tw.keyColumns
			e.keyCmpFuncs = tw.keyCmpFuncs
			go util.WithRecovery(func() {
				defer wg.Done()
				tw.run(ctx)
			}, nil)
		}
		for {
			childRowChk := e.children[0].newFirstChunk()

			err := e.children[0].Next(ctx, chunk.NewRecordBatch(childRowChk))
			if err != nil {
				return errors.Trace(err)
			}
			if childRowChk.NumRows() == 0 {
				break
			}
			chkCh <- childRowChk
		}
		close(chkCh)
		wg.Wait()
		e.fetched = true
	}

	for !req.IsFull() && uint64(e.idx) < e.totalLimit {

		j := 0
		//log.Infof("e.idx %d worker idx %d worker len %d", e.idx, e.workerRowIdx[j], len(*e.workerRowPtrs[j]))
		for j < e.concurrency && uint64(e.workerRowIdx[j]) >= e.workerRowLen[j] {
			j++
		}
		if j >= e.concurrency {
			break
		}

		//log.Infof("workerID %d workerrowPtrslen %d workerIdx %d", j, len(*e.workerRowPtrs[j]), e.workerRowIdx[j])
		minRowPtr := (*e.workerRowPtrs[j])[e.workerRowIdx[j]]
		for i := j + 1; i < e.concurrency; i++ {
			if uint64(e.workerRowIdx[i]) < e.workerRowLen[i] {
				flag := false
				keyRowJ := e.workerChunks[j].GetRow(minRowPtr)
				keyRowI := e.workerChunks[i].GetRow((*e.workerRowPtrs[i])[e.workerRowIdx[i]])
				flag = e.lessRow(keyRowJ, keyRowI)
				if !flag {
					minRowPtr = (*e.workerRowPtrs[i])[e.workerRowIdx[i]]
					j = i
				}
			}
		}
		e.workerRowIdx[j]++
		e.idx++
		//log.Infof("workerID %d idx %d offsetIdx %d limitOffset %d RowPtr %v",j, e.idx, e.offsetIdx, e.limit.Offset, minRowPtr)
		if e.limit.Offset == 0 || uint64(e.offsetIdx)  == e.limit.Offset  {
			req.AppendRow(e.workerChunks[j].GetRow(minRowPtr))
		} else {
			e.offsetIdx++
		}
	}
	return nil
}

func (e *topNWorker) initPointers() {
	e.rowPtrs = make([]chunk.RowPtr, 0, e.rowChunks.Len())
	e.memTracker.Consume(int64(8 * e.rowChunks.Len()))
	for chkIdx := 0; chkIdx < e.rowChunks.NumChunks(); chkIdx++ {
		rowChk := e.rowChunks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			e.rowPtrs = append(e.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
}

func (e *TopNExec) loadChunksUntilTotalLimit(ctx context.Context) (*chunk.List, error) {

	rowChunks := chunk.NewList(e.retTypes(), e.initCap, e.maxChunkSize)
	rowChunks.GetMemTracker().AttachTo(e.memTracker)
	rowChunks.GetMemTracker().SetLabel("rowChunks")
	for uint64(rowChunks.Len()) < e.totalLimit {
		srcChk := e.children[0].newFirstChunk()
		// adjust required rows by total limit
		srcChk.SetRequiredRows(int(e.totalLimit-uint64(rowChunks.Len())), e.maxChunkSize)
		err := e.children[0].Next(ctx, chunk.NewRecordBatch(srcChk))
		if err != nil {
			return rowChunks, errors.Trace(err)
		}
		if srcChk.NumRows() == 0 {
			break
		}
		rowChunks.Add(srcChk)
	}

	return rowChunks, nil
}

const topNCompactionFactor = 4

func (t *topNWorker) processChildChk(childRowChk *chunk.Chunk) error {
	for i := 0; i < childRowChk.NumRows(); i++ {
		heapMaxPtr := t.rowPtrs[0]
		var heapMax, next chunk.Row
		heapMax = t.rowChunks.GetRow(heapMaxPtr)
		next = childRowChk.GetRow(i)
		if t.chkHeap.greaterRow(heapMax, next) {
			// Evict heap max, keep the next row.
			t.rowPtrs[0] = t.rowChunks.AppendRow(childRowChk.GetRow(i))
			heap.Fix(t.chkHeap, 0)
		}
	}
	return nil
}

// doCompaction rebuild the chunks and row pointers to release memory.
// If we don't do compaction, in a extreme case like the child data is already ascending sorted
// but we want descending top N, then we will keep all data in memory.
// But if data is distributed randomly, this function will be called log(n) times.
//func (e *TopNExec) doCompaction() error {
//	newRowChunks := chunk.NewList(e.retTypes(), e.initCap, e.maxChunkSize)
//	newRowPtrs := make([]chunk.RowPtr, 0, e.rowChunks.Len())
//	for _, rowPtr := range e.rowPtrs {
//		newRowPtr := newRowChunks.AppendRow(e.rowChunks.GetRow(rowPtr))
//		newRowPtrs = append(newRowPtrs, newRowPtr)
//	}
//	newRowChunks.GetMemTracker().SetLabel("rowChunks")
//	e.memTracker.ReplaceChild(e.rowChunks.GetMemTracker(), newRowChunks.GetMemTracker())
//	e.rowChunks = newRowChunks
//
//	e.memTracker.Consume(int64(-8 * len(e.rowPtrs)))
//	e.memTracker.Consume(int64(8 * len(newRowPtrs)))
//	e.rowPtrs = newRowPtrs
//	return nil
//}
