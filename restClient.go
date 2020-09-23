package yukonquery

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

const (
	MethodGET  = "GET"
	MethodPOST = "POST"
	MethodPUT  = "PUT"
)

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
