package db

import (
	"fmt"
	"time"

	"github.com/GoGraph/dbConn"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"

	"cloud.google.com/go/spanner" //v1.21.0
	"google.golang.org/grpc/codes"
)
)

const (
	logid = "EventDB: "
)

var (
	dynSrv *dynamodb.DynamoDB
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
	//dynSrv = params.Service
}

// func logEvent(e Event) error {

//         ctx := context.Background()
//         client, err := spanner.NewClient(ctx, db)
//         if err != nil {
//                 return err
//         }
// 		defer client.Close()
		
// 		switch x:=e.(type) {

// 		case AttachNode:
// 			sql1=`INSERT into EventLog (eId, tag, seq, status, start) VALUES (@eid, @tag, @seq, @status, @start)`
// 			params1=map[string]interface{}{ 
// 				"eid":   x.eID,
//                 "tag":   x.tag,
// 				"seq":   x.seq,
// 				"status": x.status,
// 				"start":  x.start,
// 			}
// 			sql2=`INSERT into NodeAttachDetachEvent(pUID, cUID, Sortk) values (@puid,@cuid,@sortk)`
// 			params2=map[string]interface{}{ 
// 				"puid":   x.puid,
// 				"cuid":   x.cuid,
// 				"sortk"	: x.sortk,
// 			}

// 		case DetachNode:

// 		}

//        	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {

// 		// execute all mutatations in single batch
// 		t0 := time.Now()
// 		rowcount, err := tx.BatchUpdate(ctx, stmts)
// 		t1 := time.Now()
// 		if err != nil {
// 			fmt.Println("Batch update errored: ", err)
// 			return err
// 		}
// 		fmt.Printf("%v rowcount for BatchUpdate:", rowcount)

// 		return nil
// 	})
		 

// }

func logEvent(e Event, status string, duration string, errEv ...error) error {

	    // baseEvent:=func(e Event, mode tx.StdDML) {
		// 	x:=e.(event)
		// 	mut:=tx.NewMutation(EventTbl,nil,nil,mode )
		// 	mut.AddMember("eID",x.eID)
		// 	mut.AddMember("seq",x.seq)
		// 	mut.AddMember("status",x.status)
		// 	mut.AddMember("start",x.start)
		// 	mut.AddMember("dur",duration)
		// 	if len(errEv) > 0 {
		// 		mut.AddMember("err",errEv[0].Error())
		// 	}
		// 	x.tx.Add(mut)
		// }

		x.Event.LogEvent(duration,errEv)

		switch x:=e.(type) {

		case event: 

			mut:=tx.NewMutation(EventTbl,nil,nil,mode )
			mut.AddMember("eID",x.eID)
			mut.AddMember("seq",x.seq)
			mut.AddMember("status",x.status)
			mut.AddMember("start",x.start)
			mut.AddMember("dur",duration)
			if len(errEv) > 0 {
				mut.AddMember("err",errEv[0].Error())
			}
			x.tx.Add(mut)

		case AttachNode:

			//baseEvent(x.Event)

			mut=tx.NewMutation(ANEventTbl,nil,nil,tx.Insert )
			mut.AddMember("eID",x.eID)
			mut.AddMember("cuid",x.cuid)
			mut.AddMember("puid",x.puid)
			mut.AddMember("sortk",x.sortk)
			x.tx.Add(mut)

			x.tx.Execute()
		}


}
