package db

import (
	"context"
	"fmt"

	elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"

	"cloud.google.com/go/spanner" //v1.21.0
)

func MakeRunId(logid string, program string) (id int64, err error) {

	var rec struct {
		Id int64 `spanner: run`
	}

	stmt := spanner.Statement{SQL: `Select max(run) from mon_run `, Params: map[string]interface{}{}}
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

	runid := rec.Id + 1
	status := "R"
	for i := 0; i < 5; i++ {
		rtx := tx.New("Run")
		m := rtx.NewInsert(tbl.Monrun).AddMember("run", runid).AddMember("start", "$CURRENT_TIMESTAMP$").AddMember("Program", logid).AddMember("Status", status)
		m.AddMember("program", program).AddMember("logfile", slog.GetLogfile())
		rtx.Add(m)

		err = rtx.Execute()
		if err == nil {
			// TODO: check for duplicate error
			return runid, nil
		}
		runid++
	}

	return 0, err

}

func FinishRun(runid int64) {
	rtx := tx.New("Run")

	status := "C"
	if elog.RunErrored() {
		status = "E"
	}
	m := rtx.NewMutation(tbl.Monrun, util.UID(nil), "", mut.Update).AddMember("run", runid, mut.IsKey).AddMember("finish", "$CURRENT_TIMESTAMP$")
	rtx.Add(m.AddMember("Status", status))

	err := rtx.Execute()
	if err != nil {
		fmt.Printf("Error in FinishRun(): %s", err)
	}
}
