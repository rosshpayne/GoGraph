package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	//"github.com/GoGraph/dbs"
	elog "github.com/GoGraph/rdf/errlog"
	"github.com/GoGraph/tx/mut"
	//"github.com/GoGraph/util"

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
		if col.Opr == mut.Inc {
			sql.WriteString(col.Name)
			sql.WriteByte('+')
		}

		switch c {

		case "Nd", "XF", "Id", "XBl", "L*":
			switch col.Opr {
			case mut.Set:
				sql.WriteString(col.Param)

			default:
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
		panic(fmt.Errorf("db execute error: opr is no assigned"))
		return ggMutation{}
	}

}

func Execute(bs []*mut.Mutations, tag string) error {
	// GoGraph mutations
	var (
		ggms  []ggMutation
		bggms [][]ggMutation
	)

	// generate statements for each mutation
	syslog(fmt.Sprintf("execute: bs: batches %d\n", len(bs)))
	for i, v := range bs {
		syslog(fmt.Sprintf("execute: bs: %s batch %d mutations %d\n", tag, i, len(*v)))
	}

	for _, b := range bs {

		for _, m := range *b {

			y, ok := m.(*mut.Mutation)
			switch ok {
			case true:
				// generate inbuilt "standard" SQL
				ggms = append(ggms, genSQLStatement(y, y.GetOpr()))

			case false:
				// generate SQL from client source
				var spnStmts []spanner.Statement

				for _, s := range m.GetStatements() {
					stmt := spanner.Statement{SQL: s.SQL, Params: s.Params}
					spnStmts = append(spnStmts, stmt)
				}
				ggms = append(ggms, ggMutation{spnStmts, false})
			}
		}
		bggms = append(bggms, ggms)
		ggms = nil
	}
	// for i, v := range bggms {
	// 	syslog(fmt.Sprintf("execute3: tag %s batch %d mutations %d\n", tag, i, len(v)))
	// 	syslog(fmt.Sprintf("bggm: %#v\n", v))
	// }
	// log dmls
	// syslog(fmt.Sprintf("Tag  %s   Mutations: %d\n", tag, len(bggms), len(bggms[0])))
	// if len(ggms) == 0 {
	// 	return fmt.Errorf("No statements executed...")
	// }
	// batch statements excluding second stmt in merge dml.
	var (
		retryTx bool
		// bundled stmts
		stmts      []spanner.Statement
		mergeRetry []*spanner.Statement
		// batch of bundled statments
		bStmts [][]spanner.Statement
		bRetry [][]*spanner.Statement
	)
	//combine stmts and any merge alternate-SQL into separate slices
	for _, ggms := range bggms {
		retryTx = false
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
		// create a batch
		bStmts = append(bStmts, stmts)
		bRetry = append(bRetry, mergeRetry)
		stmts, mergeRetry = nil, nil
	}

	// var (
	// 	s     strings.Builder
	// 	uuids string
	// )
	// for _, stmts := range bStmts {

	// 	for _, v := range stmts {
	// 		for k, kv := range v.Params {
	// 			switch k {
	// 			//case "Nd", "pk", "opk", "PKey", "cuid", "puid":
	// 			case "pk", "PKey":
	// 				switch x := kv.(type) {
	// 				case []byte:
	// 					uuids = util.UID(x).String()
	// 				case [][]uint8:
	// 					for _, x := range x {
	// 						uuids = util.UID(x).String()
	// 					}

	// 				}
	// 			}
	// 		}
	// 	}
	// 	s.WriteByte('[')
	// 	s.WriteString(uuids)
	// 	s.WriteByte(']')
	// 	s.WriteString("Params: ")
	// 	params := s.String()
	// 	s.Reset()
	// 	s.WriteByte('[')
	// 	s.WriteString(uuids)
	// 	s.WriteByte(']')
	// 	s.WriteString("Stmt: ")
	// 	stmt := s.String()
	// 	s.Reset()
	// 	for i, v := range stmts {
	// 		syslog(fmt.Sprintf("%s %d sql: %s\n", stmt, i, v.SQL))
	// 		syslog(fmt.Sprintf("%s %#v\n", params, v.Params))
	// 	}
	// }
	ctx := context.Background()
	//
	// apply to database using BatchUpdate
	//
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// execute mutatations in single batch
		var retry bool
		for i, stmts := range bStmts {
			retry = false
			for len(stmts) > 0 {

				t0 := time.Now()
				rowcount, err := txn.BatchUpdate(ctx, stmts)
				t1 := time.Now()
				if err != nil {
					elog.Add(logid, err)
					return err
				}
				mergeRetry := bRetry[i]
				syslog(fmt.Sprintf("BatchUpdate[%d]: Elapsed: %s, Stmts: %d  %v  MergeRetry: %d retry: %v\n", i, t1.Sub(t0), len(stmts), rowcount, len(mergeRetry), retry))
				stmts = nil
				// check status of any merged sql statements
				//apply merge retry stmt if first merge stmt resulted in no rows processed
				if retry {
					break
				}
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
		}

		return nil

	})

	return err
}
