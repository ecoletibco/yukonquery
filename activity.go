package yukonquery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
)

type YukonConnection struct {
	Id              string            `json:"id"`
	Token           string            `json:"token"`
	ConnectorName   string            `json:"connectorName"`
	ConnectionProps map[string]string `json:"connectionProps"`
	IsConnected     bool              `json:"isConnected"`
	Error           string            `json:"error"`
}

type UcsConnection struct {
	Id              string            `json:"id"`
	Token           string            `json:"token"`
	IsConnected     bool              `json:"isConnected"`
	Error           string            `json:"error"`	
}

type YukonQueryResponse struct {
	Id      string        `json:"id"`
	EOF     bool          `json:"eof"`
	Results []interface{} `json:"results"`
}

type Activity struct {
	settings *Settings
	client   *http.Client
	queryObj Query
}

func init() {
	_ = activity.Register(&Activity{}, New)
}

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

	client, err := getHttpClient(20)
	if err != nil {
		return nil, err
	}

	connectionId, connectionToken, err := connect(client, s)
	if err != nil {
		return nil, err
	}

	queryObj, err := parseQuery(s.Query)
	if err != nil {
		return nil, err
	}

	queryObj.ConnectionId = connectionId
	queryObj.ConnectionToken = connectionToken

	act := &Activity{
		settings: s,
		client:   &client,
		queryObj: *queryObj,
	}

	return act, nil
}

func (a *Activity) Cleanup() error {

	if a.queryObj.ConnectionId != "" {
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

	queryResponse, err := executeQuery(*a.client, a.settings, a.queryObj)
	if err != nil {
		return false, err
	}

	err = ctx.SetOutput("eof", queryResponse.EOF)
	if err != nil {
		return false, err
	}

	err = ctx.SetOutput("results", queryResponse.Results)
	if err != nil {
		return false, err
	}

	return true, nil
}

func connect(client http.Client, s *Settings) (string, string, error) {

	if s.URL == "" {
		return "", "", fmt.Errorf("'url' is required")
	}

	if s.UcsConnectionId != "" {
		connectionId, connectionToken, err := connectViaUCS(client, s)
		if err != nil {
			return "", "", err
		}
		return connectionId, connectionToken, nil
	} else {
		connectionId, connectionToken, err := connectNative(client, s)
		if err != nil {
			return "", "", err
		}
		return connectionId, connectionToken, nil
	}
}

func connectNative(client http.Client, s *Settings) (string, string, error) {

	if s.ConnectorName == "" {
		return "", "", fmt.Errorf("'connectorName' is required")
	}

	yukonConn := &YukonConnection{
		ConnectorName:   s.ConnectorName,
		ConnectionProps: s.ConnectorProps,
	}

	baseUrl := s.URL
	uri := baseUrl + "/connections"

	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"

	reqBodyJSON, err := json.Marshal(yukonConn)
	if err != nil {
		return "", "", err
	}
	reqBody := bytes.NewBuffer([]byte(reqBodyJSON))

	resp, err := getRestResponse(client, MethodPOST, uri, headers, reqBody)
	if err != nil {
		return "", "", err
	}

	err = json.NewDecoder(resp.Body).Decode(&yukonConn)
	if err != nil {
		return "", "", err
	}

	if yukonConn.IsConnected == false {
		if yukonConn.Error != "" {
			return "", "", fmt.Errorf(yukonConn.Error)
		} else {
			return "", "", fmt.Errorf("connection failed")
		}
	}

	return yukonConn.Id, yukonConn.Token, nil
}

func connectViaUCS(client http.Client, s *Settings) (string, string, error) {

	if s.UcsConnectionToken == "" {
		return "", "", fmt.Errorf("'ucsConnectionToken' is required")
	}

	var connectionId = ""
	var connectionToken = ""

	return connectionId, connectionToken, nil
}

func executeQuery(client http.Client, s *Settings, queryObject Query) (*YukonQueryResponse, error) {

	// "/connections/e40b3c7f-bfe5-4f41-aabc-36b086aae1fc/query/account?$select=*&$top=5"

	baseUrl := s.URL
	uri := baseUrl + fmt.Sprintf("/connections/%s/query/%s?$select=%s", queryObject.ConnectionId, queryObject.From, queryObject.Select)

	if queryObject.Top != "" {
		uri += fmt.Sprintf("&$top=%s", queryObject.Top)
	}
	if queryObject.Skip != "" {
		uri += fmt.Sprintf("&$skip=%s", queryObject.Skip)
	}
	if queryObject.Where != "" {
		uri += fmt.Sprintf("&$filter=%s", queryObject.Where)
	}
	if queryObject.Orderby != "" {
		uri += fmt.Sprintf("&$orderby=%s", queryObject.Orderby)
	}

	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["Token"] = queryObject.ConnectionToken

	resp, err := getRestResponse(client, MethodGET, uri, headers, nil)
	if err != nil {
		return nil, err
	}

	queryResponse := YukonQueryResponse{}
	err = json.NewDecoder(resp.Body).Decode(&queryResponse)
	if err != nil {
		return nil, err
	}

	return &queryResponse, nil
}
