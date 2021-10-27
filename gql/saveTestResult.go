package gql

import (
	"time"

	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
)

func SaveTestResult(test string, status string, nodes int, levels []int, parseET, execET string, msg string, json string, fetches int, abort bool) {

	if abort {
		return
	}

	when := time.Now().String()
	stx := tx.New("Testresults")
	smut := mut.NewInsert("TestLog").AddMember(when[:21], mut.IsKey).AddMember("Status", status).AddMember("Nodes", nodes)
	smut.AddMember("Levels", levels)
	smut.AddMember("ParseET", parseET)
	smut.AddMember("ExectET", execET)
	smut.AddMember("Json", json)
	smut.AddMember("DBread", fetches)
	smut.AddMember("Msg", msg)
	//a := Item{When: when[:21], Test: test, Status: status, Nodes: nodes, Levels: levels, ParseET: parseET, ExecET: execET, Json: json, DBread: fetches, Msg: msg}

	stx.Add(smut)

	stx.Execute()

}
