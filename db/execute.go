package db

import (
	"context"
	"crypto/x509"
	"log"
	"strings"
	"time"

	"github.com/GoGraph/dbConn"
	"github.com/GoGraph/params.go"
	"github.com/GoGraph/tx"

	//"google.golang.org/api/spanner/v1"

	"cloud.google.com/go/spanner" //v1.21.0
	"google.golang.org/grpc/codes"
)

var client spanner.Client

func init() {

	client := dbConn.New()

}

func Execute(tx *tx.Handle) error {

	tx.TransactionStart = time.Now()

	tx.Err = executeTransaction(tx.GetMutations())

	tx.TransactionEnd = time.Now()

	return tx.Err
}

//
func genSQLUpdate(mut tx.Mutation, params map[string]interface{}, sql strings.Builder) string {

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
	}

	//
	params[col.Param] = col.Value
	sql.WriteString(` where PKey = @pk and SortK = @sk`)

	return sql.String()

}

func genSQLInsertWithValues(mut tx.Mutation, params map[string]interface{}, sql strings.Builder) string {

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(stmt.tbl)
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
		sql.WriteString(set.param)

		params[set.param] = set.value
	}

	return sql.String()
}

func genSQLInsertWithSelect(mut tx.Mutation, params map[string]interface{}, sql strings.Builder) string {

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(stmt.tbl)
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
		sql.WriteString(set.param)

		params[set.param] = set.value
	}
	sql.WriteString(" from dual where 0 = (select count(PKey) from PropagatedScalar where PKey=@pk and SortK=@sk) ")

	return sql.String()
}

func genSQLStatement(mut tx.Mutation, opr StdDML) (spnStmt []spanner.Statement) {
	var (
		params map[string]interface{}
		stmt   spanner.Statement
	)

	switch opr {

	case tx.Update, tx.Append:

		params = map[string]interface{}{"pk": mut.pk, "sk": mut.sk}

		stmt.Sql = spanner.NewStatement(genSQLUpdate(mut, params, sql))
		stmt.Params = params

		stmts := make([]spanner.Statement, 1)
		stmts[0] = stmt

	case tx.Insert:

		params = make(map[string]interface{})

		stmt.Sql = spanner.NewStatement(genSQLInsertWithValues(mut, params, sql))
		stmt.Params = params

		stmts := make([]spanner.Statement, 1)
		stmts[0] = stmt

	case tx.Merge, tx.PropagateMerge:

		params = map[string]interface{}{"pk": mut.pk, "sk": mut.sk}

		stmt.Sql = spanner.NewStatement(genSQLUpdate(mut, params, sql))
		stmt.Params = params

		stmts := make([]spanner.Statement, 2)
		stmts[0] = stmt

		stmt.Sql = spanner.NewStatement(genSQLInsertWithSelect(mut, params, sql))
		stmt.Params = params
		stmts[1] = stmt

	}

	return stmts
}

func executeTransaction(muts []tx.Mutation) error {

	ctx := context.Background()
	var stmts []spanner.Statement

	// generate statements for each mutation
	for _, mut := range muts {

		switch x := mut.GetOpr().(type) {

		case StdDML:

			stmts = append(stmts, genSQLStatement(mut, x.Opr)...)

		case IdSet:

			upd := spanner.Statement{
				SQL: "update edge set Id = @id where PKey=@pk and Sortk = @sk",
				Params: map[string]interface{}{
					"pk": mut.pk,
					"sk": mut.sk,
					"id": x.Value,
				},
			}

			stmts = append(stmts, upd)

		case XFSet:

			upd := spanner.Statement{
				SQL: "update edge set XF = @xf where PKey=@pk and Sortk = @sk",
				Params: map[string]interface{}{
					"pk": mut.pk,
					"sk": mut.sk,
					"xf": x.Value,
				},
			}

			stmts = append(stmts, upd)

		case WithOBatchLimit: // used only for Append Child UID to overflow batch as it sets XF value in parent UID-pred

			// append child UID to Nd
			cuid := make([][]byte, 1)
			cuid[0] = x.Cuid
			xf := make([]int, 1)
			xf[0] = blk.ChildUID

			upd1 := spanner.Statement{
				SQL: "update edge set ARRAY_CONCAT(Nd,@cuid), ARRAY_CONCAT(XF,@status) where PKey=@pk and Sortk = @sk",
				Params: map[string]interface{}{
					"pk":     x.TUID,
					"sk":     x.Osortk,
					"cuid":   cuid,
					"status": xf,
				},
			}
			// set OvflItemFull if array size reach bufferLimit
			xf := x.DI.GetXF()
			xf[x.NdIndex] = blk.OvflItemFull
			upd2 := spanner.Statement{
				SQL: "update edge x set XF=@xf where PKey=@pk and Sortk = @sk and @size = (select ARRAY_LENGTH(XF) from edge  SortK  = @osk and PKey = @opk)",
				Params: map[string]interface{}{
					"pk":   x.DI.PKey,
					"sk":   x.DI.GetSortK(),
					"opk":  x.TUID,
					"osk":  x.Osortk,
					"size": params.OvfwBatchSize,
				},
			}
			stmts = append(stmts, upd1, upd2)

		}
	}
	// log stmts
	fmt.Println("Mutations: ", len(muts))
	for _, s := range stmts {
		fmt.Println("SQL: ", s.SQL)
	}

	// apply transactions to database using BatchUpdate
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {

		// execute all mutatations in single batch
		t0 := time.Now()
		rowcount, err := tx.BatchUpdate(ctx, stmts)
		t1 := time.Now()
		if err != nil {
			fmt.Println("Batch update errored: ", err)
			return err
		}
		fmt.Printf("%v rowcount for BatchUpdate:", rowcount)

		return nil
	})

	return err
}
