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

func New() (*spanner.Client, error) {
	// Note: NewClient does not error if instance is not available. Error is generated when db is accessed in type.graphShortName
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, "projects/banded-charmer-310203/instances/test-instance/databases/test-sdk-db")
	if err != nil {
		logerr(err)
		return nil, err
	}
	return client, nil
}
