package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/GoGraph/block"
	param "github.com/GoGraph/dygparam"
	//"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/mut"

	//"google.golang.org/api/spanner/v1"

	"cloud.google.com/go/spanner" //v1.21.0
)

// GoGraph SQL Mutations - used to identify merge SQL. Spanner has no merge sql so we use a combination of update-insert processing.
type ggMutation struct {
	stmt    []spanner.Statement
	isMerge bool
}

//
func genSQLUpdate(m *mut.Mutation, params map[string]interface{}) string {

	var sql strings.Builder
	sql.WriteString(`update `)
	sql.WriteString(m.GetTable())
	sql.WriteString(` set `)

	for i, col := range m.GetMembers()[2:] {

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

		case "Nd", "XF", "Id", "XBl", "L*":
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
		params[col.Param[1:]] = col.Value
	}

	//
	col := m.GetMembers()[0]
	sql.WriteString(" where ")
	sql.WriteString(col.Name)
	sql.WriteString(" = ")
	sql.WriteString(col.Param)
	params[col.Param[1:]] = col.Value
	col = m.GetMembers()[1]
	if col.Name != "__" {
		sql.WriteString(" and ")
		sql.WriteString(col.Name)
		sql.WriteString(" = ")
		sql.WriteString(col.Param)
		params[col.Param[1:]] = col.Value
	}

	return sql.String()

}

func genSQLInsertWithValues(m *mut.Mutation, params map[string]interface{}) string {

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(m.GetTable())
	sql.WriteString(` (`)

	for i, col := range m.GetMembers() {
		// does table have a sortk
		if col.Name == "__" {
			// no it doesn't
			continue
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(col.Name)
	}
	sql.WriteString(`) values (`)
	for i, set := range m.GetMembers() {
		// does table have a sortk
		if set.Name == "__" {
			// no it doesn't
			continue
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(set.Param)

		params[set.Param[1:]] = set.Value
	}
	sql.WriteByte(')')

	return sql.String()
}

func genSQLInsertWithSelect(m *mut.Mutation, params map[string]interface{}) string {

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(m.GetTable())
	sql.WriteString(` (`)

	for i, col := range m.GetMembers() {

		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(col.Name)
	}
	sql.WriteByte(')')
	sql.WriteString(` values ( `)
	for i, set := range m.GetMembers() {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(set.Param)

		params[set.Param[1:]] = set.Value
	}
	sql.WriteByte(')')
	//sql.WriteString(" from dual where NOT EXISTS (select 1 from EOP where PKey=@PKey and SortK=@SortK) ")

	return sql.String()
}

func genSQLStatement(m *mut.Mutation, opr mut.StdDML) ggMutation {
	var params map[string]interface{}

	// 	type dml struct {
	//   	stmt [2]spanner.Statement
	// 	    merge bool
	// }
	switch opr {

	case mut.Update, mut.Append:

		params = make(map[string]interface{}) //{"pk": m.GetPK(), "sk": m.GetSK()}

		stmt := spanner.NewStatement(genSQLUpdate(m, params))
		stmt.Params = params

		//stmts = make([]spanner.Statement, 1)
		return ggMutation{stmt: []spanner.Statement{stmt}} // stmt

	case mut.Insert:

		params = make(map[string]interface{})

		stmt := spanner.NewStatement(genSQLInsertWithValues(m, params))
		stmt.Params = params
		return ggMutation{stmt: []spanner.Statement{stmt}}

	case mut.Merge:

		params = make(map[string]interface{}) //{"pk": m.GetPK(), "sk": m.GetSK()}

		s1 := spanner.NewStatement(genSQLUpdate(m, params))
		s1.Params = params

		s2 := spanner.NewStatement(genSQLInsertWithSelect(m, params))
		s2.Params = params

		return ggMutation{stmt: []spanner.Statement{s1, s2}, isMerge: true}

	default:
		return ggMutation{}
	}

}

//func Execute(ms mut.Mutations) error {
func Execute(ms []*mut.Mutation, tag string) error {
	// GoGraph mutations
	var ggms []ggMutation

	// generate statements for each mutation
	for _, m := range ms {

		switch x := m.GetOpr().(type) {

		case mut.StdDML:

			// for _, ggm := range genSQLStatement(m, x) {
			// 	ggms = append(ggms, ggm)
			// }
			ggms = append(ggms, genSQLStatement(m, x))

		case mut.IdSet:

			upd := spanner.Statement{
				SQL: "update EOP set Id = @id where PKey=@pk and Sortk = @sk",
				Params: map[string]interface{}{
					"pk": m.GetPK(),
					"sk": m.GetSK(),
					"id": x.Value,
				},
			}
			ggms = append(ggms, ggMutation{stmt: []spanner.Statement{upd}})

		case mut.XFSet:

			upd := spanner.Statement{
				SQL: "update EOP set XF = @xf where PKey=@pk and SortK = @sk",
				Params: map[string]interface{}{
					"pk": m.GetPK(),
					"sk": m.GetSK(),
					"xf": x.Value,
				},
			}

			ggms = append(ggms, ggMutation{stmt: []spanner.Statement{upd}})

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
			xf := make([]int64, 1)
			xf[0] = int64(block.ChildUID)

			upd1 := spanner.Statement{
				SQL: "update EOP set Nd=ARRAY_CONCAT(Nd,@cuid), XF=ARRAY_CONCAT(XF,@status) where PKey=@pk and SortK = @sk",
				Params: map[string]interface{}{
					"pk":     x.Ouid,
					"sk":     x.OSortK,
					"cuid":   cuid,
					"status": xf,
				},
			}
			// set OBatchSizeLimit if array size reaches param.OvflBatchSize
			xf = x.DI.XF // or GetXF()
			xf[x.Index] = block.OBatchSizeLimit
			upd2 := spanner.Statement{
				SQL: "update EOP x set XF=@xf where PKey=@pk and Sortk = @sk and @size = (select ARRAY_LENGTH(XF) from EOP  where SortK  = @osk and PKey = @opk)",
				Params: map[string]interface{}{
					"pk":   x.DI.Pkey,
					"sk":   x.DI.GetSortK(),
					"xf":   xf,
					"opk":  x.Ouid,
					"osk":  x.OSortK,
					"size": param.OvfwBatchSize,
				},
			}
			ggms = append(ggms, ggMutation{stmt: []spanner.Statement{upd1, upd2}})

		default:
			panic(fmt.Errorf("db.Execute mutation opr not catered for. "))
		}
	}
	//dmls = dmls[:1]
	// log dmls
	syslog(fmt.Sprintf("Tag  %s   Mutations: %d\n", tag, len(ggms)))
	if len(ggms) == 0 {
		return fmt.Errorf("No statements executed...")
	}

	// batch statements excluding second stmt in merge dml.
	var (
		stmts      []spanner.Statement
		retryStmts []spanner.Statement
	)
	var retryTx bool
	for _, ggm := range ggms {

		switch ggm.isMerge {
		case true:
			retryTx = true
			stmts = append(stmts, ggm.stmt[0])
			retryStmts = append(retryStmts, ggm.stmt[1])
		default:
			stmts = append(stmts, ggm.stmt...)
			for range ggm.stmt {
				retryStmts = append(retryStmts, spanner.Statement{})
			}
		}
	}
	for i, s := range stmts {
		syslog(fmt.Sprintf("Stmt %d sql: %s\n", i, s.SQL))
		syslog(fmt.Sprintf("Params: %#v\n", s.Params))
	}
	if !retryTx {
		retryStmts = nil
	}

	ctx := context.Background()
	//
	// apply to database using BatchUpdate
	//
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {

		// execute mutatations in single batch
		for {

			t0 := time.Now()
			rowcount, err := txn.BatchUpdate(ctx, stmts)
			t1 := time.Now()
			if err != nil {
				syslog(fmt.Sprintln("Batch update error: ", err))
				syslog(fmt.Sprintln("len(rowcount): ", len(rowcount)))
				for i, v := range rowcount {
					fmt.Println("stmt: ", i, v)
				}
				return err
			}
			syslog(fmt.Sprintf("%v rowcount for BatchUpdate: - len(stmts) %d \n", rowcount, len(stmts)))
			syslog(fmt.Sprintf("Elapsed time for BatchUpdate: %s\n", t1.Sub(t0)))
			// check status of any merged sql statements
			var retry bool
			for i, r := range retryStmts {
				if len(r.SQL) != 0 {
					// retry merge SQL insert stmt if update sql updated zero rows.
					if rowcount[i] == 0 {
						retry = true
						break
					}
				}
			}
			if retry {
				stmts = nil
				// run all merge retry stmts in curren transaction
				for _, retry := range retryStmts {
					if len(retry.SQL) != 0 {
						stmts = append(stmts, retry)
					}
				}
				// nullify retryStmts to prevent processing in second retry loop
				retryStmts = nil
			} else {
				break
			}
		}

		return nil
	})

	return err
}
