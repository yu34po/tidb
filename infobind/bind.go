package infobind

import (
	"fmt"
	"runtime"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	log "github.com/sirupsen/logrus"
)

var _ Manager = (*BindManager)(nil)

// User implements infobind.Manager interface.
// This is used to update or check Ast.
type BindManager struct {
	is                 infoschema.InfoSchema
	currentDB          string
	SessionHandle      *Handle //session handle
	*Handle                    //global handle
	GlobalBindAccessor GlobalBindAccessor
	copy               bool
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

// Manager is the interface for providing bind related operations.
type Manager interface {
	GetMatchedAst(sql, db string) *BindData
	MatchHint(originalNode ast.Node, is infoschema.InfoSchema, db string)

	GetAllSessionBindData() []*BindData
	GetAllGlobalBindData() []*BindData
	AddSessionBind(originSql, bindSql, defaultDb string, bindAst ast.StmtNode) error
	AddGlobalBind(originSql, bindSql, defaultDb, charset, collation string) error
	RemoveSessionBind(originSql string, defaultDb string) error
	RemoveGlobalBind(originSql string, defaultDb string) error
}

const key keyType = 0

// BindManager binds Manager to context.
func BindBinderManager(ctx sessionctx.Context, pc Manager) {
	ctx.SetValue(key, pc)
}

// GetBindManager gets Checker from context.
func GetBindManager(ctx sessionctx.Context) Manager {
	if v, ok := ctx.Value(key).(Manager); ok {
		return v
	}
	return nil
}
func (b *BindManager) GetMatchedAst(sql string, db string) *BindData {
	bc := b.Handle.Get()
	if bindArray, ok := bc.Cache[sql]; ok {
		for _, v := range bindArray {
			if v.Status != 1 {
				continue
			}
			if len(v.Db) == 0 {
				return v
			}
			if v.Db == db {
				return v
			}
		}
	}
	return nil
}

func (b *BindManager) deleteBind(hash, db string) {
	bc := b.Handle.Get()
	if bindArray, ok := bc.Cache[hash]; ok {
		for _, v := range bindArray {
			if v.Db == db {
				v.Status = -1
				break
			}
		}
	}
	b.Handle.bind.Store(bc)
}

func isPrimaryIndexHint(indexName model.CIStr) bool {
	return indexName.L == "primary"
}

func checkIndexName(paths []*model.IndexInfo, idxName model.CIStr, tblInfo *model.TableInfo) bool {

	for _, path := range paths {
		if path.Name.L == idxName.L {
			return true
		}
	}
	if isPrimaryIndexHint(idxName) && tblInfo.PKIsHandle {
		return true
	}
	return false
}

func checkHint(indexHints []*ast.IndexHint, tblInfo *model.TableInfo) bool {

	publicPaths := make([]*model.IndexInfo, 0, len(tblInfo.Indices)+1)
	for _, index := range tblInfo.Indices {
		if index.State == model.StatePublic {
			publicPaths = append(publicPaths, index)
		}
	}
	for _, hint := range indexHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}
		for _, idxName := range hint.IndexNames {
			if checkIndexName(publicPaths, idxName, tblInfo) {
				return true
			}
		}

	}
	return false
}

func (b *BindManager) dataSourceBind(originalNode, hintedNode *ast.TableName) error {

	if len(hintedNode.IndexHints) == 0 {
		return nil
	}

	dbName := originalNode.Schema
	if dbName.L == "" {
		dbName = model.NewCIStr(b.currentDB)
	}

	tbl, err := b.is.TableByName(dbName, originalNode.Name)
	if err != nil {
		errMsg := fmt.Sprintf("table %s or Db %s not exist", originalNode.Name.L, dbName.L)
		return errors.New(errMsg)
	}

	tableInfo := tbl.Meta()
	ok := checkHint(hintedNode.IndexHints, tableInfo)
	if !ok {
		errMsg := fmt.Sprintf("table %s missing hint", tableInfo.Name)
		return errors.New(errMsg)
	}
	if b.copy {
		originalNode.IndexHints = append(originalNode.IndexHints, hintedNode.IndexHints...)
	} else {
		originalNode.IndexHints = nil
	}
	return nil
}

func (b *BindManager) joinBind(originalNode, hintedNode *ast.Join) error {
	if originalNode.Right == nil {

		return b.resultSetNodeBind(originalNode.Left, hintedNode.Left)
	}

	err := b.resultSetNodeBind(originalNode.Left, hintedNode.Left)
	if err != nil {
		return err
	}

	return b.resultSetNodeBind(originalNode.Right, hintedNode.Right)

}
func (b *BindManager) unionSelectBind(originalNode, hintedNode *ast.UnionStmt) error {
	selects := originalNode.SelectList.Selects
	for i := len(selects) - 1; i >= 0; i-- {
		err := b.selectBind(selects[i], hintedNode.SelectList.Selects[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BindManager) resultSetNodeBind(originalNode, hintedNode ast.ResultSetNode) error {
	switch x := originalNode.(type) {
	case *ast.Join:
		return b.joinBind(x, hintedNode.(*ast.Join))
	case *ast.TableSource:
		ts, _ := hintedNode.(*ast.TableSource)

		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			return b.selectBind(v, ts.Source.(*ast.SelectStmt))
		case *ast.UnionStmt:
			return b.unionSelectBind(v, hintedNode.(*ast.TableSource).Source.(*ast.UnionStmt))
		case *ast.TableName:
			return b.dataSourceBind(v, ts.Source.(*ast.TableName))

		}
	case *ast.SelectStmt:
		return b.selectBind(x, hintedNode.(*ast.SelectStmt))
	case *ast.UnionStmt:
		return b.unionSelectBind(x, hintedNode.(*ast.UnionStmt))
	default:
	}
	return nil
}

func (b *BindManager) selectionBind(where ast.ExprNode, hintedWhere ast.ExprNode) error {
	switch v := where.(type) {
	case *ast.SubqueryExpr:
		if v.Query != nil {
			return b.resultSetNodeBind(v.Query, hintedWhere.(*ast.SubqueryExpr).Query)
		}
	case *ast.ExistsSubqueryExpr:
		if v.Sel != nil {
			return b.resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, hintedWhere.(*ast.ExistsSubqueryExpr).Sel.(*ast.SubqueryExpr).Query)
		}
	case *ast.PatternInExpr:
		if v.Sel != nil {
			return b.resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, hintedWhere.(*ast.PatternInExpr).Sel.(*ast.SubqueryExpr).Query)
		}
	}
	return nil

}

func (b *BindManager) selectBind(originalNode, hintedNode *ast.SelectStmt) error {
	if hintedNode.TableHints != nil {
		if b.copy {
			originalNode.TableHints = append(originalNode.TableHints, hintedNode.TableHints...)
		} else {
			originalNode.TableHints = nil
		}
	}
	if originalNode.From != nil {

		err := b.resultSetNodeBind(originalNode.From.TableRefs, hintedNode.From.TableRefs)
		if err != nil {
			return err
		}
	}
	if originalNode.Where != nil {
		return b.selectionBind(originalNode.Where, hintedNode.Where)
	}
	return nil
}

func (b *BindManager) doTravel(originalNode, hintedNode ast.Node) error {
	switch x := originalNode.(type) {
	case *ast.SelectStmt:
		return b.selectBind(x, hintedNode.(*ast.SelectStmt))

	}
	return nil
}

func (b *BindManager) MatchHint(originalNode ast.Node, is infoschema.InfoSchema, db string) {
	var (
		hintedNode ast.Node
		hash       string
	)
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("hint panic stack is:\n%s", buf)
			b.copy = false
			b.doTravel(originalNode, hintedNode)
			b.deleteBind(hash, db)
		}
	}()

	bc := b.SessionHandle.Get()
	sql := originalNode.Text()
	hash = parser.Digest(sql)

	if bindArray, ok := bc.Cache[hash]; ok {
		for _, v := range bindArray {
			if v.Status != 1 {
				continue
			}
			if len(v.Db) == 0 || v.Db == db {
				hintedNode = v.ast
			}
		}
	}
	if hintedNode == nil {
		bc = b.Handle.Get()
		if bindArray, ok := bc.Cache[hash]; ok {
			for _, v := range bindArray {
				if v.Status != 1 {
					continue
				}
				if len(v.Db) == 0 || v.Db == db {
					hintedNode = v.ast
				}
			}
		}

		if hintedNode == nil {
			log.Warnf("sql %s try match hint failed", sql)
			return
		}
	}

	b.currentDB = db
	b.is = is
	b.copy = true
	err := b.doTravel(originalNode, hintedNode)
	if err != nil {
		b.copy = false
		b.doTravel(originalNode, hintedNode)
		b.deleteBind(hash, db)
		log.Warnf("sql %s try match hint failed %v", sql, err)

	}
	log.Warnf("sql %s try match hint success", sql)

	return
}

func (b *BindManager) AddSessionBind(originSql, bindSql, defaultDb string, bindAst ast.StmtNode) error {
	bindRecord := bindRecord{
		OriginalSql: originSql,
		BindSql:     bindSql,
		Db:          defaultDb,
		Status:      1,
		CreateTime: types.Time{
			Time: types.FromGoTime(time.Now()),
			Type: mysql.TypeTimestamp,
			Fsp:  types.DefaultFsp,
		},
		UpdateTime: types.Time{
			Time: types.FromGoTime(time.Now()),
			Type: mysql.TypeTimestamp,
			Fsp:  types.DefaultFsp,
		},
	}

	bindData := &BindData{
		bindRecord: bindRecord,
		ast:        bindAst,
	}

	hash := parser.Digest(originSql)
	oldBindDataArr, ok := b.SessionHandle.Get().Cache[hash]
	if ok {
		for idx, oldBindData := range oldBindDataArr {
			if oldBindData.bindRecord.OriginalSql == bindRecord.OriginalSql && oldBindData.bindRecord.Db == bindRecord.Db {
				if oldBindData.Status == 1 {
					return errors.Trace(errors.New(fmt.Sprintf("%s bind alreay exist", originSql)))
				} else {
					oldBindDataArr = append(oldBindDataArr[:idx], oldBindDataArr[idx+1:]...)
					break
				}
			}
		}
	}

	oldBindDataArr = append(oldBindDataArr, bindData)
	b.SessionHandle.Get().Cache[hash] = oldBindDataArr

	return nil
}

func (b *BindManager) AddGlobalBind(originSql, bindSql, defaultDb, charset, collation string) error {
	return b.GlobalBindAccessor.AddGlobalBind(originSql, bindSql, defaultDb, charset, collation)
}

func (b *BindManager) RemoveSessionBind(originSql string, defaultDb string) error {
	hash := parser.Digest(originSql)

	oldBindDataArr, ok := b.SessionHandle.Get().Cache[hash]
	if ok {
		for idx, oldBindData := range oldBindDataArr {
			if oldBindData.bindRecord.OriginalSql == originSql && oldBindData.bindRecord.Db == defaultDb {
				oldBindDataArr = append(oldBindDataArr[:idx], oldBindDataArr[idx+1:]...)
				break
			}
		}

		if len(oldBindDataArr) != 0 {
			b.SessionHandle.Get().Cache[hash] = oldBindDataArr
		} else {
			delete(b.SessionHandle.Get().Cache, hash)
		}
	}

	return nil
}

func (b *BindManager) RemoveGlobalBind(originSql string, defaultDb string) error {
	return b.GlobalBindAccessor.DropGlobalBind(originSql, defaultDb)
}

func (b *BindManager) GetAllSessionBindData() []*BindData {
	var bindDataArr []*BindData
	for _, bindData := range b.SessionHandle.Get().Cache {
		bindDataArr = append(bindDataArr, bindData...)
	}

	return bindDataArr
}

func (b *BindManager) GetAllGlobalBindData() []*BindData {
	var bindDataArr []*BindData

	for _, tempBindDataArr := range b.Get().Cache {
		for _, bindData := range tempBindDataArr {
			bindDataArr = append(bindDataArr, bindData)
		}
	}

	return bindDataArr
}

type GlobalBindAccessor interface {
	DropGlobalBind(originSql string, defaultDb string) error
	AddGlobalBind(originSql string, bindSql string, defaultDb string, charset string, collation string) error
}
