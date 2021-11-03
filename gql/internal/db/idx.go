package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/GoGraph/dbConn"
	mon "github.com/GoGraph/gql/monitor"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"

	"cloud.google.com/go/spanner" //v1.21.0
)

type Equality int

const (
	logid = "gqlDB: "
)
const (
	EQ Equality = iota + 1
	LT
	GT
	GE
	LE
)

var opc = map[Equality]string{EQ: "=", LT: "<", GT: ">", GE: ">=", LE: "<="}

// api for GQL query functions

type NodeResult struct {
	PKey  util.UID
	SortK string
	Ty    string
}

type (
	QResult  []NodeResult
	AttrName = string
)

var (
	err error
	//tynames   []tyNames
	//tyShortNm map[string]string
)

var (
	client *spanner.Client
)

func init() {
	client, err = dbConn.New()
	if err != nil {
		syslog(fmt.Sprintf("Cannot create a db Client: %s", err.Error()))
		panic(err)
	}
}

func GetClient() *spanner.Client {
	return client
}
func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, e.Error(), true)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

func syslog(s string) {
	slog.Log(logid, s)
}

func RootCnt(ty string, cnt int, sk string, opr Equality) (QResult, error) {

	var rows int
	var all QResult
	var sql strings.Builder

	sql.WriteString(`select e.PKey, e.Sortk, e.Ty 
		from eop e 
		where e.Sortk = @sk 
		and e.Ty = @ty
		and e.CNT `)
	sql.WriteString(opc[opr])
	sql.WriteString(` @cnt`)

	params := map[string]interface{}{"ty": ty, "sk": sk, "cnt": cnt, "gr": types.GraphSN()}
	stmt := spanner.Statement{SQL: sql.String(), Params: params}
	ctx := context.Background()
	t0 := time.Now()
	iter := client.Single().Query(ctx, stmt)

	err = iter.Do(func(r *spanner.Row) error {
		rows++
		rec := NodeResult{}
		err := r.ToStruct(&rec)
		if err != nil {
			return err
		}
		all = append(all, rec)

		return nil
	})
	t1 := time.Now()
	if err != nil {
		return nil, err
	}
	fmt.Println("Rows: ", rows)
	//
	// send stats
	//
	v := mon.Fetch{CapacityUnits: 0, Items: rows, Duration: t1.Sub(t0)}
	stat := mon.Stat{Id: mon.DBFetch, Value: &v}
	mon.StatCh <- stat

	return all, nil

}

var basesql = `select b.Ty, ns.PKey, ns.Sortk
	from NodeScalar ns
	join Block b using (PKey)
	where ns.P = @P and `

// gattr generates a P (<graphShortName>|<attrName>) value (as used in table NodeScalar)
func gattr(attr string) string {
	var ga strings.Builder
	ga.WriteString(types.GraphSN())
	ga.WriteByte('|')
	ga.WriteString(attr)
	return ga.String()
}

func GSIQueryS(attr AttrName, lv string, op Equality) (QResult, error) {

	var sql strings.Builder
	sql.WriteString(basesql)
	sql.WriteString("ns.S ")
	sql.WriteString(opc[op])
	sql.WriteString(" @V")

	param := map[string]interface{}{"P": gattr(attr), "V": lv}

	return query(sql.String(), param)
}

func GSIQueryI(attr AttrName, lv int64, op Equality) (QResult, error) {

	var sql strings.Builder
	sql.WriteString(basesql)
	sql.WriteString("ns.I ")
	sql.WriteString(opc[op])
	sql.WriteString(" @V")

	param := map[string]interface{}{"P": gattr(attr), "V": lv}

	return query(sql.String(), param)
}

func GSIQueryF(attr AttrName, lv float64, op Equality) (QResult, error) {

	var sql strings.Builder
	sql.WriteString(basesql)
	sql.WriteString("ns.F ")
	sql.WriteString(opc[op])
	sql.WriteString(" @V")

	param := map[string]interface{}{"P": gattr(attr), "V": lv}

	return query(sql.String(), param)
}

func GSIhasS(attr AttrName) (QResult, error) {

	sql := `select ns.PKey, ns.SortK, b.Ty
		from nodescalar ns
		join block b using (PKey)
		where ns.P = @P and ns.S is not null`

	param := map[string]interface{}{"P": gattr(attr)}

	return query(sql, param)
}

func GSIhasN(attr AttrName) (QResult, error) {

	sql := `select ns.PKey, ns.SortK, b.Ty
		from nodescalar ns
		join block b using (PKey)
		where ns.P = @P and ns.N is not null`

	param := map[string]interface{}{"P": gattr(attr)}

	return query(sql, param)

}

func GSIhasChild(attr AttrName) (QResult, error) {

	sql := `select ns.PKey, ns.SortK, b.Ty
		from nodescalar ns
		join block b using (PKey)
		where ns.P = @P and ns.ASZ > 1`

	param := map[string]interface{}{"P": gattr(attr)}

	return query(sql, param)

}

func query(sql string, params map[string]interface{}) (QResult, error) {

	var (
		all  QResult
		rows int
	)

	stmt := spanner.Statement{SQL: sql, Params: params}
	ctx := context.Background()
	t0 := time.Now()
	iter := client.Single().Query(ctx, stmt)

	err = iter.Do(func(r *spanner.Row) error {
		rows++
		rec := NodeResult{}
		err := r.ToStruct(&rec)
		if err != nil {
			return err
		}
		all = append(all, rec)

		return nil
	})
	t1 := time.Now()
	if err != nil {
		return nil, err
	}
	//
	// send stats
	//
	v := mon.Fetch{CapacityUnits: 0, Items: rows, Duration: t1.Sub(t0)}
	stat := mon.Stat{Id: mon.DBFetch, Value: &v}
	mon.StatCh <- stat

	return all, nil
}
