package gql

import (
	"fmt"
	"time"

	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
)

func SaveTestResult(test string, status string, nodes int, levels []int, parseET, execET string, msg string, json string, fetches int, abort bool) {

	if abort {
		return
	}

	when := time.Now().String()
	tx := New("SaveTestResults")
	mut := NewMutation("TestLog", when[:21], nil, tx.Insert)
	mut.AddMember("Status", status)
	mut.AddMember("Nodes", nodes)
	mut.AddMember("Levels", levels)
	mut.AddMember("ParseET", parseET)
	mut.AddMember("ExectET", execET)
	mut.AddMember("Json", json)
	mut.AddMember("DBread", fetches)
	mut.AddMember("Msg", msg)
	//a := Item{When: when[:21], Test: test, Status: status, Nodes: nodes, Levels: levels, ParseET: parseET, ExecET: execET, Json: json, DBread: fetches, Msg: msg}

	tx.Add(mut)

	tx.Execute()

}
