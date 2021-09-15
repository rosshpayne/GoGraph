package db

import (
	"context"
	"fmt"

	"github.com/GoGraph/dbConn"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"

	"cloud.google.com/go/spanner" //v1.21.0
)

const (
	batchsize = 100
	logid     = "DB: "
)

var (
	err         error
	rows        int64
	unprocessed [][]byte
	client      *spanner.Client
)

func init() {
	client = dbConn.New()
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

type Unprocessed struct {
	PKey []byte `spanner:"PKey"`
}

type DataT struct {
	Uid util.UID
	Eod bool
}

func ScanForDPitems(attr string, dpCh chan<- DataT) {

	stmt := spanner.Statement{SQL: `Select PKey from Block where Ty = @ty and IX = "X"`, Params: map[string]interface{}{"ty": attr}}
	ctx := context.Background()
	iter := client.Single().Query(ctx, stmt)

	err = iter.Do(func(r *spanner.Row) error {
		rows++

		rec := Unprocessed{}
		err := r.ToStruct(&rec)
		if err != nil {
			fmt.Println("ToStruct error: %s", err.Error())
			return err
		}
		// blocking enqueue on channel - limited number of handling processors will force a wait on channel
		dpCh <- DataT{util.UID(rec.PKey), false}

		return nil

	})

}
