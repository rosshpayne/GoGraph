package db

import (
	"context"
	"fmt"

	"github.com/GoGraph/db"
	elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"

	"cloud.google.com/go/spanner" //v1.21.0
)

const (
	batchsize = 50
	logid     = "DB: "
)

var (
	err    error
	client *spanner.Client
)

// func init() {
// 	client, _ = dbConn.New()
// }
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

type rec struct {
	PKey    []byte `spanner:"PKey"`
	Ty      string
	IxValue string `spanner:"P"`
	Value   string `spanner:"S"`
}

type batch struct {
	Eod       bool // End-of-data
	FetchCh   chan *rec
	BatchCh   chan batch
	LoadAckCh chan struct{}
}

//go db.ScanForESattrs(tysn, sk, FetchCh)

func NewBatch() batch {
	return batch{FetchCh: make(chan *rec), BatchCh: make(chan batch), LoadAckCh: make(chan struct{})}
}

func ScanForESentry(ty string, sk string, batch batch, saveCh chan<- struct{}, saveAckCh <-chan struct{}) {

	// load all type ty data into all slice.
	var all []*rec

	slog.Log("DB:", fmt.Sprintf("ScanForESitems for type %q", ty))

	sql := `Select b.Ty, ns.PKey, ns.P, ns.S 
			from Block b 
			left join eslog l using (PKey)
			inner join NodeScalar ns using (PKey) 
			where b.Ty = @ty and ns.Sortk = @sortk 
			and l.Pkey is null limit @limit`
	params := map[string]interface{}{"ty": ty, "sortk": sk, "limit": batchsize}
	client := db.GetClient()

	for {

		var eod bool
		stmt := spanner.Statement{SQL: sql, Params: params}
		ctx := context.Background()
		iter := client.Single().Query(ctx, stmt)

		err = iter.Do(func(r *spanner.Row) error {

			rec := &rec{}
			err := r.ToStruct(rec)
			if err != nil {
				return err
			}
			all = append(all, rec)

			return nil
		})
		if err != nil {
			elog.Add("DB:", err)
		}
		slog.Log("DPDB:", fmt.Sprintf("Unprocessed records for type %q: %d", ty, len(all)))

		if len(all) < batchsize {
			eod = true
		}

		for _, v := range all {
			// blocking enqueue on channel - limited number of handling processors will force a wait on channel
			syslog(fmt.Sprintf("v: %#v", v))
			batch.FetchCh <- v
		}
		close(batch.FetchCh)

		batch.Eod = eod
		batch.FetchCh = make(chan *rec)
		// sync with load es
		batch.BatchCh <- batch
		<-batch.LoadAckCh
		// save log data to database
		saveCh <- struct{}{}
		<-saveAckCh

		all = nil

		if eod {
			break
		}

	}

}
