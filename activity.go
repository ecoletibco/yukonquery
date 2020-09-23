package yukonquery

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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

const (
	SELECT    = "select"
	ALL       = "*"
	TOP       = "top"
	SKIP      = "skip"
	FROM      = "from"
	WHERE     = "where"
	ORDERBY   = "orderby"
	ASCENDING = "asc"
	DECENDING = "desc"
)

const (
	EQUAL            = "eq"
	NOT_EQUAL        = "ne"
	GREATER          = "gt"
	GREATER_OR_EQUAL = "ge"
	LESSER           = "lt"
	LESSER_OR_EQUAL  = "le"
)

const (
	AND = "and"
	OR  = "or"
	NOT = "not"
)

type YukonConnection struct {
	Id              string            `json:"id"`
	Token           string            `json:"token"`
	ConnectorName   string            `json:"connectorName"`
	ConnectionProps map[string]string `json:"connectionProps"`
	IsConnected     bool              `json:"isConnected"`
	Error           string            `json:"error"`
}

type YukonQueryResponse struct {
	Id      string        `json:"id"`
	EOF     bool          `json:"eof"`
	Results []interface{} `json:"results"`
}

type Query struct {
	ConnectionId    string
	ConnectionToken string
	Select          string
	Top             string
	Skip            string
	From            string
	Where           string
	Orderby         string
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
		queryObj: queryObj,
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

func parseQuery(queryString string) (Query, error) {

	var queryObj = Query{}

	queryString = strings.ReplaceAll(queryString, ",", " ")
	queryString = strings.TrimSpace(queryString)
	queryString = strings.ToLower(queryString)
	if queryString == "" {
		return queryObj, fmt.Errorf("'query' is required")
	}

	selectIndex := -1
	topIndex := -1
	skipIndex := -1
	fromIndex := -1
	whereIndex := -1
	orderbyIndex := -1

	queryParts := strings.Split(queryString, " ")
	for i, queryPart := range queryParts {
		switch queryPart {
		case SELECT:
			selectIndex = i
		case TOP:
			topIndex = i
		case SKIP:
			skipIndex = i
		case FROM:
			fromIndex = i
		case WHERE:
			whereIndex = i
		case ORDERBY:
			orderbyIndex = i
		}
	}

	if selectIndex != 0 {
		return queryObj, fmt.Errorf("invalid query: only select statements are supported")
	}

	if fromIndex == -1 {
		return queryObj, fmt.Errorf("invalid query: a from clause is required")
	}

	if fromIndex+1 >= len(queryParts) {
		return queryObj, fmt.Errorf("invalid query: table name is required")
	}

	if topIndex != -1 {
		return queryObj, fmt.Errorf("invalid query: top not supported")
	}

	if skipIndex != -1 {
		return queryObj, fmt.Errorf("invalid query: skip not supported")
	}

	if whereIndex != -1 {
		return queryObj, fmt.Errorf("invalid query: where not supported")
	}

	if orderbyIndex != -1 {
		return queryObj, fmt.Errorf("invalid query: orderby not supported")
	}

	// parse for column names
	columnNames := ""
	subParts := queryParts[selectIndex+1:]
	for _, queryPart := range subParts {
		if queryPart == "" {
			continue
		} else if queryPart == ALL {
			columnNames = ALL
			break
		} else if queryPart == TOP {
			break
		} else if queryPart == SKIP {
			break
		} else if queryPart == FROM {
			break
		} else if queryPart == WHERE {
			break
		} else if queryPart == ORDERBY {
			break
		} else {
			if columnNames != "" {
				columnNames += ", "
			}
			columnNames += queryPart
		}
	}
	if columnNames == "" {
		return queryObj, fmt.Errorf("invalid query: select requires column list or * for all")
	}
	queryObj.Select = columnNames

	// parse for table name
	tableName := ""
	subParts = queryParts[fromIndex+1:]
	for _, queryPart := range subParts {
		if queryPart == "" {
			continue
		} else if queryPart == ALL {
			break
		} else if queryPart == TOP {
			break
		} else if queryPart == SKIP {
			break
		} else if queryPart == FROM {
			break
		} else if queryPart == WHERE {
			break
		} else if queryPart == ORDERBY {
			break
		} else {
			tableName = queryPart
		}
	}
	if tableName == "" {
		return queryObj, fmt.Errorf("invalid query: table name not found")
	}
	queryObj.From = tableName

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
