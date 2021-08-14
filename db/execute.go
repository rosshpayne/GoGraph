package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/GoGraph/block"
	"github.com/GoGraph/dygparam"
	"github.com/GoGraph/tx/mut"

	//"google.golang.org/api/spanner/v1"

	"cloud.google.com/go/spanner" //v1.21.0
)

//
func genSQLUpdate(mut *mut.Mutation, params map[string]interface{}) string {

	var sql strings.Builder
	sql.WriteString(`update `)
	sql.WriteString(mut.GetTable())
	sql.WriteString(`set `)

	for i, col := range mut.GetMembers() {

		if i != 0 {
			sql.WriteByte(',')
		}
		var c string // generatlised column name
		sql.WriteString(col.Name)
		sql.WriteByte('=')
		if col.Name[0] == 'L' {
			c = "L*"
		} else {
			c = col.Name
		}
		switch c {

		case "Nd", "XF", "Id", "L*":
			// Array (List) attributes - append/concat to type
			sql.WriteString("ARRAY_CONCAT(")
			sql.WriteString(col.Name)
			sql.WriteByte(',')
			sql.WriteString(col.Param)
			sql.WriteByte(')')

		default:
			// non-Array attributes - set
			sql.WriteString(col.Param)
		}
		params[col.Param] = col.Value
	}

	//
	sql.WriteString(` where PKey = @pk and SortK = @sk`)

	return sql.String()

}

func genSQLInsertWithValues(mut *mut.Mutation, params map[string]interface{}) string {

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(mut.GetTable())
	sql.WriteString(` ( `)

	for i, col := range mut.GetMembers() {

		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(col.Name)
	}
	sql.WriteString(` values (@pk, @sk, `)
	for i, set := range mut.GetMembers() {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(set.Param)

		params[set.Param] = set.Value
	}

	return sql.String()
}

func genSQLInsertWithSelect(mut *mut.Mutation, params map[string]interface{}) string {

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(mut.GetTable())
	sql.WriteString(` ( `)

	for i, col := range mut.GetMembers() {

		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(col.Name)
	}
	sql.WriteString(` select `)
	for i, set := range mut.GetMembers() {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(set.Param)

		params[set.Param] = set.Value
	}
	sql.WriteString(" from dual where 0 = (select count(PKey) from PropagatedScalar where PKey=@pk and SortK=@sk) ")

	return sql.String()
}

func genSQLStatement(m *mut.Mutation, opr mut.StdDML) (spnStmt []spanner.Statement) {
	var (
		params map[string]interface{}
		stmt   spanner.Statement
		stmts  []spanner.Statement
	)

	switch opr {

	case mut.Update, mut.Append:

		params = map[string]interface{}{"pk": m.GetPK(), "sk": m.GetSK()}

		stmt = spanner.NewStatement(genSQLUpdate(m, params))
		stmt.Params = params

		stmts = make([]spanner.Statement, 1)
		stmts[0] = stmt

	case mut.Insert:

		params = make(map[string]interface{})

		stmt = spanner.NewStatement(genSQLInsertWithValues(m, params))
		stmt.Params = params

		stmts := make([]spanner.Statement, 1)
		stmts[0] = stmt

	case mut.Merge:

		params = map[string]interface{}{"pk": m.GetPK(), "sk": m.GetSK()}

		stmt = spanner.NewStatement(genSQLUpdate(m, params))
		stmt.Params = params

		stmts := make([]spanner.Statement, 2)
		stmts[0] = stmt

		stmt = spanner.NewStatement(genSQLInsertWithSelect(m, params))
		stmt.Params = params
		stmts[1] = stmt

	}

	return stmts
}

func Execute(ms mut.Mutations) error {

	var stmts []spanner.Statement

	// generate statements for each mutation
	for _, m := range ms {

		switch x := m.GetOpr().(type) {

		case mut.StdDML:

			for _, v := range genSQLStatement(m, x) {
				stmts = append(stmts, v)
			}

		case mut.IdSet:

			upd := spanner.Statement{
				SQL: "update edge set Id = @id where PKey=@pk and Sortk = @sk",
				Params: map[string]interface{}{
					"pk": m.GetPK(),
					"sk": m.GetSK(),
					"id": x.Value,
				},
			}

			stmts = append(stmts, upd)

		case mut.XFSet:

			upd := spanner.Statement{
				SQL: "update edge set XF = @xf where PKey=@pk and Sortk = @sk",
				Params: map[string]interface{}{
					"pk": m.GetPK(),
					"sk": m.GetSK(),
					"xf": x.Value,
				},
			}

			stmts = append(stmts, upd)

		case mut.WithOBatchLimit: // used only for Append Child UID to overflow batch as it sets XF value in parent UID-pred
			// Ouid   util.UID
			// Cuid   util.UID
			// Puid   util.UID
			// DI     *blk.DataItem
			// OSortK string // overflow sortk
			// Index  int    // UID-PRED Nd index entry
			// append child UID to Nd
			cuid := make([][]byte, 1)
			cuid[0] = x.Cuid
			xf := make([]int, 1)
			xf[0] = block.ChildUID

			upd1 := spanner.Statement{
				SQL: "update edge set ARRAY_CONCAT(Nd,@cuid), ARRAY_CONCAT(XF,@status) where PKey=@pk and Sortk = @sk",
				Params: map[string]interface{}{
					"pk":     x.Ouid,
					"sk":     x.OSortK,
					"cuid":   cuid,
					"status": xf,
				},
			}
			// set OvflItemFull if array size reach bufferLimit
			xf = x.DI.XF // or GetXF()
			xf[x.Index] = block.OvflItemFull
			upd2 := spanner.Statement{
				SQL: "update edge x set XF=@xf where PKey=@pk and Sortk = @sk and @size = (select ARRAY_LENGTH(XF) from edge  SortK  = @osk and PKey = @opk)",
				Params: map[string]interface{}{
					"pk":   x.DI.GetPkey(),
					"sk":   x.DI.GetSortK(),
					"opk":  x.Ouid,
					"osk":  x.OSortK,
					"size": params.OvfwBatchSize,
				},
			}
			stmts = append(stmts, upd1, upd2)

		}
	}
	// log stmts
	fmt.Println("Mutations: ", len(ms))
	for _, s := range stmts {
		fmt.Println("SQL: ", s.SQL)
	}
	ctx := context.Background()
	// apply transactions to database using BatchUpdate
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {

		// execute all mutatations in single batch
		t0 := time.Now()
		rowcount, err := txn.BatchUpdate(ctx, stmts)
		t1 := time.Now()
		if err != nil {
			fmt.Println("Batch update errored: ", err)
			return err
		}
		fmt.Printf("%v rowcount for BatchUpdate:", rowcount)
		fmt.Printf("Elapsed time for BatchUpdate:", t1.Sub(t0))

		return nil
	})

	return err
}
