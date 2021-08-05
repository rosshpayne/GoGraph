package db

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner" //v1.21.0
	"golang.org/x/tools/0.20210608163600-9ed039809d4c/go/analysis/passes/nilfunc"
	"google.golang.org/grpc/codes"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/dbConn"
	param "github.com/GoGraph/dygparam"
	slog "github.com/GoGraph/syslog"
)

const (
	logid    = "TypesDB: "
	typesTbl = param.TypesTable
)

type tyNames struct {
	ShortNm string `spanner:"SName"`
	LongNm  string `spanner:"Name"`
}

var (
	graphNm   string
	gId       string // graph Identifier (graph short name). Each Type name is prepended with the graph id. It is stripped off when type data is loaded into caches.
	err       error
	tynames   []tyNames
	tyShortNm map[string]string
)

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

func init() {

	dynSrv = dbConn.New()

}

func SetGraph(graph_ string) {
	fmt.Println("In db SetGraph.....", graph_)
	graphNm = graph_

	tynames, err = loadTypeShortNames()
	if err != nil {
		panic(err)
	}
	fmt.Println("short Names: ", tynames)
	//
	// populate type short name cache. This cache is conccurent safe as it is readonly from now on.
	//
	tyShortNm = make(map[string]string)
	for _, v := range tynames {
		tyShortNm[v.LongNm] = v.ShortNm
	}
	for k, v := range tyShortNm {
		fmt.Println("ShortNames: ", k, v)
	}

}

func GetTypeShortNames() ([]tyNames, error) {
	return tynames, nil
}

func LoadDataDictionary(graphNm string) (blk.TyIBlock, error) {

	var tyNm tyNames

	// 	type TyItem struct {
	// 	Nm   string   `json:"PKey"`  // type name e.g m.Film, r.Person
	// 	Atr  string   `json:"SortK"` // attribute name
	// 	Ty   string   // DataType
	// 	F    []string // facets name#DataType#CompressedIdentifer
	// 	C    string   // compressed identifer for attribute
	// 	P    string   // data partition containig attribute data - TODO: is this obselete???
	// 	Pg   bool     // true: propagate scalar data to parent
	// 	N    bool     // NULLABLE. False : not null (attribute will always exist ie. be populated), True: nullable (attribute may not exist)
	// 	Cd   int      // cardinality - NOT USED
	// 	Sz   int      // average size of attribute data - NOT USED
	// 	Ix   string   // supported indexes: FT=Full Text (S type only), "x" combined with Ty will index in GSI Ty_Ix
	// 	IncP []string // (optional). List of attributes to be propagated. If empty all scalars will be propagated.
	// 	//	cardinality string   // 1:N , 1:1
	// }

	ctx := context.Backgroud()
	//defer client.Close()

	params := map[string]interface{}{"graph": graphNm}

	// stmt returns one row
	stmt := `Select  g.SName+"."+nt.Name Nm, 
			atr.Name Atr,
			atr.SName C, 
			atr.DT Ty, 
			atr.Partition P, 
			atr.Nullable N, 
			atr.Ix 
			from Graph g join Type nt on (Id) join Attribute atr on (Id)
			where G.Name = @graph `

	iter := client.Single().Query(ctx, spanner.Statement{SQL: stmt, Params: params})
	defer iter.Stop()

	dd := make(blk.TyIBlock, iter.RowCount, iter.RowCount)

	var tyItem blk.TyItem
	for i := 0; i < iter.RowCount; i++ {
		row, err := iter.Next()
		if err != nil {
			return err
		}
		err := row.ToStruct(&tyItem)
		if err != nil {
			return err
		}

		dd[i] = tyItemm
	}

	return dd, nil
}

func loadTypeShortNames() ([]tyNames, error) {

	syslog("db.loadTypeShortNames ")

	var tyNm tyNames

	ctx := context.Backgroud()
	//defer client.Close()

	params := map[string]interface{}{"graph": graphNm}

	// stmt returns one row
	stmt := `Select name, sname
			from Graph g join Type t on (Id)
			where g.Name = @graph`

	iter := client.Single().Query(ctx, spanner.Statement{SQL: stmt, Params: params})
	defer iter.Stop()

	tyNms := make([]tyNames, iter.RowCount, iter.RowCount)

	for i := 0; i < iter.RowCount; i++ {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		err = row.ToStruct(&tyNm)
		if err != nil {
			return err
		}
		tyNms[i] = tyNm
	}

	return tyNms, nil

}
