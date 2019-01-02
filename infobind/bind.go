package infobind

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	log "github.com/sirupsen/logrus"
	"runtime"
)

var _ Manager = (*BindManager)(nil)

// User implements infobind.Manager interface.
// This is used to update or check ast.
type BindManager struct {
	is        infoschema.InfoSchema
	currentDB string
	copy      bool
	*Handle
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

// Manager is the interface for providing bind related operations.
type Manager interface {
	GetMatchedAst(sql, db string) *BindData
	MatchHint(originalNode ast.Node, is infoschema.InfoSchema, db string)
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
	if bindArray, ok := bc.cache[sql]; ok {
		for _, v := range bindArray {
			if v.status != 1 {
				continue
			}
			if len(v.db) == 0 {
				return v
			}
			if v.db == db {
				return v
			}
		}
	}
	return nil
}

func (b *BindManager) deleteBind(hash, db string) {
	bc := b.Handle.Get()
	if bindArray, ok := bc.cache[hash]; ok {
		for _, v := range bindArray {
			if v.db == db {
				v.status = -1
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
		errMsg := fmt.Sprintf("table %s or db %s not exist", originalNode.Name.L, dbName.L)
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

	bc := b.Handle.Get()
	sql := originalNode.Text()
	hash = parser.Digest(sql)

	if bindArray, ok := bc.cache[hash]; ok {
		for _, v := range bindArray {
			if v.status != 1 {
				continue
			}
			if len(v.db) == 0 || v.db == db {
				hintedNode = v.ast
			}
		}
	}
	if hintedNode == nil {
		log.Warnf("sql %s try match hint failed", sql)
		return
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
