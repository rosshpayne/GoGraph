package db

import (
	"context"
	"fmt"

	"github.com/GoGraph/attach/ds"
	"github.com/GoGraph/dbConn"
	elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"

	"cloud.google.com/go/spanner" //v1.21.0
)

const logid = "Attach/DB:"

func syslog(s string) {
	slog.Log(logid, s)
}

type Parent struct {
	Puid []byte `spanner:"Puid"`
}

var (
	client *spanner.Client
	limit  = 50
)

func init() {
	var err error
	client, err = dbConn.New()
	if err != nil {
		elog.Add(logid, err)
		panic(err)
	}
}

func ScanForNodes() (all []util.UID, eof bool) {

	var err error

	stmt := spanner.Statement{SQL: `Select Puid from Edge_ where Cnt > 0 order by Cnt desc Limit @limit`, Params: map[string]interface{}{"limit": limit}}
	ctx := context.Background()
	iter := client.Single().Query(ctx, stmt)

	err = iter.Do(func(r *spanner.Row) error {

		rec := Parent{}
		err := r.ToStruct(&rec)
		if err != nil {
			return err
		}
		all = append(all, util.UID(rec.Puid))

		return nil
	})
	if err != nil {
		elog.Add(logid, fmt.Errorf("Error while fetching in ScanForNodes: %w", err))
	}

	if len(all) < limit || len(all) == 0 {
		eof = true
	}

	slog.Log(logid, fmt.Sprintf("#Parent Nodes:  %d", len(all)))

	return all, eof

}

// FetchEdge returns the edge an unattached edge (Puid, Cuid, Sortk) for a given Puid
func FetchEdge(puid util.UID) (*ds.Edge, error) {

	var err error

	stmt := spanner.Statement{SQL: `Select Puid, Cuid, Sortk from EdgeChild_ where Puid = @puid and Status = "X" limit 1`, Params: map[string]interface{}{"puid": puid}}
	ctx := context.Background()
	iter := client.Single().Query(ctx, stmt)

	rec := ds.NewEdge()

	err = iter.Do(func(r *spanner.Row) error {

		err := r.ToStruct(rec)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		elog.Add(logid, fmt.Errorf("Error while fetching in FetchEdge: %w", err))
		return nil, err
	}

	if len(rec.Puid) == 0 {
		// due to concurrent processing no row matches above condition
		return nil, fmt.Errorf("No matching EdgeChild")
	}

	return rec, nil
}
