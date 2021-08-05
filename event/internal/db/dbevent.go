package db

import (
	"fmt"
	"time"

	"github.com/DynamoGraph/dbConn"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
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
}

func LogEvent(eventData interface{}) error {

	av, err := dynamodbattribute.MarshalMap(eventData)
	if err != nil {
		return fmt.Errorf("LogEvent for %s: %s", "Error: failed to marshal event.%s ", err.Error())
	}

	{
		t0 := time.Now()
		ret, err := dynSrv.PutItem(&dynamodb.PutItemInput{
			TableName:              aws.String("DyGEvent"),
			Item:                   av,
			ConditionExpression:    aws.String("attribute_not_exists(EID)"),
			ReturnConsumedCapacity: aws.String("TOTAL"),
		})
		t1 := time.Now()
		syslog(fmt.Sprintf("LogEvent: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
		if err != nil {
			return fmt.Errorf("LogEvent Error: PutItem for %s Error: %s", err.Error())
		}
	}
	return nil
}

func UpdateEvent(eID util.UID, status string, duration string, errEv ...error) error {

	type pKey struct {
		EID []byte
		SEQ int
	}

	upd := expression.Set(expression.Name("Status"), expression.Value(status))
	upd = upd.Set(expression.Name("Dur"), expression.Value(duration))
	if len(errEv) > 0 {
		upd = upd.Set(expression.Name("Err"), expression.Value(errEv[0].Error()))
	}
	updC := expression.Equal(expression.Name("Status"), expression.Value("C")).Not()
	//
	expr, _ := expression.NewBuilder().WithCondition(updC).WithUpdate(upd).Build()
	// TODO: Handle err
	// if err != nil {
	// 	return newDBExprErr("UpdateEvent", "", "", err)
	// }
	//
	// Marshal primary key, sortK
	//
	pkey := pKey{EID: eID, SEQ: 1}
	av, _ := dynamodbattribute.MarshalMap(&pkey)
	// TODO: Handle err
	// if err != nil {
	// 	return newDBMarshalingErr("UpdateEvent", eID.String(), "", "MarshalMap", err)
	// }
	input := &dynamodb.UpdateItemInput{
		Key:                       av,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
	}
	input = input.SetTableName("DyGEvent").SetReturnConsumedCapacity("TOTAL")
	//
	{
		t0 := time.Now()
		uio, err := dynSrv.UpdateItem(input)
		t1 := time.Now()
		syslog(fmt.Sprintf("UpdateEvent: consumed updateitem capacity: %s, Duration: %s\n", uio.ConsumedCapacity, t1.Sub(t0)))
		if err != nil {
			return err
		}
	}
	return nil
}
