package yukonquery

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
)

const (
	MethodGET  = "GET"
	MethodPOST = "POST"
	MethodPUT  = "PUT"
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
	client          *http.Client
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

	client, err := getHttpClient(20)
	if err != nil {
		return nil, err
	}

	connectionId, connectionToken, err := connect(client, s)
	if err != nil {
		return nil, err
	}

	query, err := parseQuery(s.Query)
	if err != nil {
		return nil, err
	}

	act := &Activity{
		client:          &client,
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

	if queryString == "" {
		return queryObj, fmt.Errorf("'query' is required")
	}

	return queryObj, nil
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

func getResults(queryResult interface{}) ([][]interface{}, error) {

	return nil, nil
}

////////////////////////////////////////////////////////////////////////////////////////
// Utils

func getHttpClient(timeout int) (http.Client, error) {

	client := &http.Client{}

	httpTransportSettings := &http.Transport{}

	if timeout > 0 {
		httpTransportSettings.ResponseHeaderTimeout = time.Second * time.Duration(timeout)
	}

	client.Transport = httpTransportSettings

	return *client, nil
}

func getRestResponse(client http.Client, method string, uri string, headers map[string]string, reqBody io.Reader) (*http.Response, error) {

	req, err := http.NewRequest(method, uri, reqBody)
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return resp, errors.New("Bad Response: " + resp.Status)
	}

	if resp == nil {
		return resp, errors.New("Empty Response")
	}

	return resp, nil
}

func getBodyAsText(respBody io.ReadCloser) string {

	defer func() {
		if respBody != nil {
			_ = respBody.Close()
		}
	}()

	var response = ""

	if respBody != nil {
		b := new(bytes.Buffer)
		b.ReadFrom(respBody)
		response = b.String()
	}

	return response
}

func getBodyAsJSON(respBody io.ReadCloser) (interface{}, error) {

	defer func() {
		if respBody != nil {
			_ = respBody.Close()
		}
	}()

	d := json.NewDecoder(respBody)
	d.UseNumber()
	var response interface{}
	err := d.Decode(&response)
	if err != nil {
		switch {
		case err == io.EOF:
			return nil, nil
		default:
			return nil, err
		}
	}

	return response, nil
}
