package run

import (
	"context"
	"fmt"

	"github.com/GoGraph/dbConn"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"

	"cloud.google.com/go/spanner" //v1.21.0
)

const logid = "run:"

var (
	client *spanner.Client
	runid  int64
)

func init() {
	var err error
	client, err = dbConn.New()
	if err != nil {
		slog.Log(logid, fmt.Sprintf("Cannot create a db Client: %s", err.Error()))
		panic(err)
	}
}

func New(logid string, program string) (id int64, err error) {

	var rec struct {
		Run spanner.NullInt64
	}

	stmt := spanner.Statement{SQL: `Select max(run) run from mon_run `, Params: map[string]interface{}{}}
	ctx := context.Background()

	iter := client.Single().Query(ctx, stmt)

	err = iter.Do(func(r *spanner.Row) error {

		err := r.ToStruct(&rec)
		if err != nil {
			return err
		}

		return nil
	})
	// insert a run record
	if err != nil {
		return 0, err
	}
	if rec.Run.IsNull() {
		runid = 1
	} else {
		runid = rec.Run.Int64 + 1
	}
	status := "R"
	for i := 0; i < 5; i++ {
		rtx := tx.New("Run")
		m := rtx.NewInsert(tbl.Monrun).AddMember("run", runid).AddMember("start", "$CURRENT_TIMESTAMP$").AddMember("Program", program).AddMember("Status", status)
		m.AddMember("logfile", slog.GetLogfile())
		rtx.Add(m)
		err = rtx.Execute()
		slog.Log(logid, fmt.Sprintf("Execute query"))
		if err == nil {
			// TODO: check for duplicate error
			return runid, nil
		}
		runid++
		if i == 4 {
			return 0, fmt.Errorf("Error in allocating Runid. Max tries reached.")
		}
	}
	return 0, err

}

func Finish(err error) {

	rtx := tx.New("Run")
	status := "C"
	if err != nil {
		status = "E"
	}

	m := rtx.NewMutation(tbl.Monrun, util.UID(nil), "", mut.Update).AddMember("run", runid, mut.IsKey).AddMember("finish", "$CURRENT_TIMESTAMP$")
	rtx.Add(m.AddMember("Status", status))

	err = rtx.Execute()
	if err != nil {
		fmt.Printf("Error in FinishRun(): %s", err)
	}
}

func Panic() {

	rtx := tx.New("Run")

	status := "P"

	fmt.Println("in run panic....")

	m := rtx.NewMutation(tbl.Monrun, util.UID(nil), "", mut.Update).AddMember("run", runid, mut.IsKey)
	rtx.Add(m.AddMember("Status", status).AddMember("finish", "$CURRENT_TIMESTAMP$"))

	err := rtx.Execute()
	if err != nil {
		fmt.Printf("Error in FinishRun(): %s", err)
	}
}
