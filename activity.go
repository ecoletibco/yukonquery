package yukonquery

import (
	"fmt"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
)

type YukonConnection struct {
	Id              string            `json:"id"`
	Token           string            `json:"token"`
	ConnectorName   string            `json:"connectorName"`
	ConnectionProps map[string]string `json:"connectionProps"`
	OrgId           string            `json:"orgId"`
	UserId          string            `json:"userId"`
}

type Query struct {
	ObjectName string
	Select     string
	Top        int
	Skip       int
	Where      string
	Orderby    string
}

type Activity struct {
	connectionId    string
	connectionToken string
	query           Query
}

func init() {
	_ = activity.Register(&Activity{}, New)
}

const (
	ovResults = "results"
)

var activityMd = activity.ToMetadata(&Settings{}, &Input{}, &Output{})

// Metadata implements activity.Activity.Metadata
func (a *Activity) Metadata() *activity.Metadata {
	return activityMd
}

func New(ctx activity.InitContext) (activity.Activity, error) {

	s := &Settings{}
	err := metadata.MapToStruct(ctx.Settings(), s, true)
	if err != nil {
		return nil, err
	}

	query, err := parseQuery(s.Query)
	if err != nil {
		return nil, err
	}

	connectionId, connectionToken, err := connect(s)
	if err != nil {
		return nil, err
	}

	act := &Activity{
		connectionId:    connectionId,
		connectionToken: connectionToken,
		query:           query,
	}

	return act, nil
}

func (a *Activity) Cleanup() error {

	if a.connectionId != "" {
		log.RootLogger().Tracef("cleaning up Yukon Query activity")
	}

	return nil
}

// Eval implements activity.Activity.Eval
func (a *Activity) Eval(ctx activity.Context) (done bool, err error) {

	in := &Input{}
	err = ctx.GetInputObject(in)
	if err != nil {
		return false, err
	}

	results, err := getResults(nil)
	if err != nil {
		return false, err
	}

	err = ctx.SetOutput(ovResults, results)
	if err != nil {
		return false, err
	}

	return true, nil
}

func parseQuery(queryString string) (Query, error) {

	var queryObj = Query{}

	if false {
		return queryObj, fmt.Errorf("only select statement is supported")
	}

	return queryObj, nil
}

func connect(s *Settings) (string, string, error) {

	var connectionId = ""
	var connectionToken = ""

	return connectionId, connectionToken, nil
}

func getResults(queryResult interface{}) ([][]interface{}, error) {

	return nil, nil
}
