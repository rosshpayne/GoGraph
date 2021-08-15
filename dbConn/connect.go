package dbConn

import (
	"context"

	slog "github.com/GoGraph/syslog"

	"cloud.google.com/go/spanner"
)

const (
	logid = "DBconnect: "
)

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, e.Error(), true)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

func New() *spanner.Client {

	ctx := context.Background()
	client, err := spanner.NewClient(ctx, "projects/banded-charmer-310203/instances/test-instance/databases/test-sdk-db")
	if err != nil {
		panic(err)
	}
	return client
}
