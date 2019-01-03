package infobind

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/types"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
)

type BindData struct {
	bindRecord
	ast ast.StmtNode
}

type BindCache struct {
	cache map[string][]*BindData
}

type Handle struct {
	bind atomic.Value
}

type HandleUpdater struct {
	Parser         *parser.Parser
	LastUpdateTime types.Time
	Ctx            sessionctx.Context
	*Handle
}

type bindRecord struct {
	originalSql string
	bindSql     string
	hashCode    []byte
	db          string
	status      int64
	createTime  types.Time
	updateTime  types.Time
}

func NewHandle() *Handle {
	return &Handle{}
}

func (h *Handle) Get() *BindCache {
	bc := h.bind.Load()
	if bc != nil {
		return bc.(*BindCache)
	}
	return &BindCache{
		cache: make(map[string][]*BindData, 1000),
	}
}

func (h *HandleUpdater) LoadDiff(sql string, bc *BindCache) error {
	tmp, err := h.Ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return errors.Trace(err)
	}

	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	chk := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(chk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err, record := decodeBindTableRow(row, fs)
			if err != nil {
				log.Errorf("row decode error %s", err)
				continue
			}
			log.Infof("record %v", record)
			err = bc.appendNode(h.Ctx, record, h.Parser)
			if err != nil {
				continue
			}
			if record.updateTime.Compare(h.LastUpdateTime) == 1 {
				h.LastUpdateTime = record.updateTime
			}
		}
		chk = chunk.Renew(chk, h.Ctx.GetSessionVars().MaxChunkSize)
	}

	return nil
}

func (h *HandleUpdater) Update(fullLoad bool) error {
	var (
		err error
		sql string
	)
	bc := h.Get()
	if fullLoad {
		sql = fmt.Sprintf("select * from mysql.bind_info")
	} else {
		sql = fmt.Sprintf("select * from mysql.bind_info where update_time > \"%s\"", h.LastUpdateTime.String())
	}
	log.Infof("sql %s", sql)
	err = h.LoadDiff(sql, bc)
	if err != nil {
		return errors.Trace(err)
	}

	h.bind.Store(bc)
	bc.Display()
	return nil
}

func parseSQL(sctx sessionctx.Context, parser *parser.Parser, sql string) ([]ast.StmtNode, []error, error) {
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	parser.SetSQLMode(sctx.GetSessionVars().SQLMode)
	parser.EnableWindowFunc(sctx.GetSessionVars().EnableWindowFunction)
	return parser.Parse(sql, charset, collation)
}

func decodeBindTableRow(row chunk.Row, fs []*ast.ResultField) (error, bindRecord) {
	var value bindRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "original_sql":
			value.originalSql = row.GetString(i)
		case f.ColumnAsName.L == "bind_sql":
			value.bindSql = row.GetString(i)
		case f.ColumnAsName.L == "default_db":
			value.db = row.GetString(i)
		case f.ColumnAsName.L == "status":
			value.status = row.GetInt64(i)
		case f.ColumnAsName.L == "create_time":
			var err error
			value.createTime = row.GetTime(i)
			if err != nil {
				return errors.Trace(err), value
			}
		case f.ColumnAsName.L == "update_time":
			var err error
			value.updateTime = row.GetTime(i)
			if err != nil {
				return errors.Trace(err), value
			}
		}
	}
	return nil, value
}

func (b *BindCache) appendNode(sctx sessionctx.Context, value bindRecord, sparser *parser.Parser) error {
	hash := parser.Digest(value.originalSql)
	if value.status == 0 {
		if bindArr, ok := b.cache[hash]; ok {
			if len(bindArr) == 1 {
				if bindArr[0].db == value.db {
					delete(b.cache, hash)
				}
				return nil
			}
			for idx, v := range bindArr {
				if v.db == value.db {
					b.cache[hash] = append(b.cache[hash][:idx], b.cache[hash][idx+1:]...)
				}
			}
		}
		return nil
	}

	stmtNodes, _, err := parseSQL(sctx, sparser, value.bindSql)
	if err != nil {
		log.Warnf("parse error:\n%v\n%s", err, value.bindSql)
		return errors.Trace(err)
	}

	newNode := &BindData{
		bindRecord: value,
		ast:        stmtNodes[0],
	}

	log.Infof("original sql [%s] bind sql [%s]", value.originalSql, value.bindSql)
	if bindArr, ok := b.cache[hash]; ok {
		for idx, v := range bindArr {
			if v.db == value.db {
				b.cache[hash][idx] = newNode
				return nil
			}
		}
	}
	b.cache[hash] = append(b.cache[hash], newNode)
	return nil

}

func (b *BindCache) Display() {
	for hash, bindArr := range b.cache {
		log.Infof("------------------hash entry %s-----------------------", hash)
		for _, bindData := range bindArr {
			log.Infof("%v", bindData.bindRecord)
		}
		log.Infof("------------------hash entry end -----------------------")

	}
}
