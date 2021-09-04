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

// pkg db must support mutations, Insert, Update, Merge:
// any other mutations (eg WithOBatchLimit) must be defined outside of DB and passed in (somehow)

// GoGraph SQL Mutations - used to identify merge SQL. Spanner has no merge sql so GOGraph
// uses a combination of update-insert processing.
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
		// for array types: set col=ARRAY_CONCAT(col,@param)
		if col.Name[0] == 'L' {
			c = "L*"
		} else {
			c = col.Name
		}
		switch c {

		case "Nd", "XF", "Id", "XBl", "L*":
			if col.Opr == mut.Set {
				sql.WriteString(col.Param)
			} else {
				// Array (List) attributes - append/concat to type
				sql.WriteString("ARRAY_CONCAT(")
				sql.WriteString(col.Name)
				sql.WriteByte(',')
				sql.WriteString(col.Param)
				sql.WriteByte(')')
			}
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

		// 	ggms = append(ggms, ggMutation{stmt: []spanner.Statement{upd}})

		case mut.WithOBatchLimit:

			// used only for Append Child UID to overflow batch as it sets XF value in parent UID-pred
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
				SQL: "update EOP x set XF=@xf where PKey=@pk and Sortk = @sk and @size = (select ARRAY_LENGTH(XF)-1 from EOP  where SortK  = @osk and PKey = @opk)",
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
		retryTx    bool
		stmts      []spanner.Statement
		mergeRetry []*spanner.Statement
	)
	//combine stmts and any merge alternate-SQL into separate slices
	for _, ggm := range ggms {

		switch ggm.isMerge {
		case true:
			retryTx = true
			stmts = append(stmts, ggm.stmt[0])
			mergeRetry = append(mergeRetry, &ggm.stmt[1])
		default:
			stmts = append(stmts, ggm.stmt...)
			for range ggm.stmt {
				mergeRetry = append(mergeRetry, nil)
			}
		}
	}
	if !retryTx {
		// abort any merge SQL processing by setting slice to nil
		mergeRetry = nil
	}
	for i, s := range stmts {
		syslog(fmt.Sprintf("Stmt %d sql: %s\n", i, s.SQL))
		syslog(fmt.Sprintf("Params: %#v\n", s.Params))
	}
	ctx := context.Background()
	//
	// apply to database using BatchUpdate
	//
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// execute mutatations in single batch
		for len(stmts) > 0 {

			t0 := time.Now()
			rowcount, err := txn.BatchUpdate(ctx, stmts)
			t1 := time.Now()
			if err != nil {
				syslog(fmt.Sprintln("Batch update error: ", err))
				return err
			}
			stmts = nil
			syslog(fmt.Sprintf("BatchUpdate: Elapsed: %s, Stmts: %d  %v  MergeRetry: %d\n", t1.Sub(t0), len(stmts), rowcount, len(mergeRetry)))
			// check status of any merged sql statements
			//apply merge retry stmt if first merge stmt resulted in no rows processed
			var retry bool
			for i, r := range mergeRetry {
				if r != nil {
					// retry merge-insert if merge-update processed zero rows.
					if rowcount[i] == 0 {
						retry = true
						break
					}
				}
			}
			if retry {
				// run all merge alternate stmts
				for _, mergeSQL := range mergeRetry {
					if mergeSQL != nil {
						stmts = append(stmts, *mergeSQL)
					}
				}
				// nullify mergeRetry to prevent processing in merge-retry execute
				mergeRetry = nil
			}
		}

		return nil

	})

	return err
}
