package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"pegasus/log"
)

const (
	MIME_TEXT = "application/text"
	MIME_JSON = "application/json"
)

type HttpUrl struct {
	IP    string
	Port  int
	Uri   string
	Query url.Values
}

func (url *HttpUrl) String() string {
	if len(url.Query) > 0 {
		query := url.Query.Encode()
		return fmt.Sprintf("http://%s:%d%s?%s",
			url.IP, url.Port, url.Uri, query)
	} else {
		return fmt.Sprintf("http://%s:%d%s",
			url.IP, url.Port, url.Uri)
	}
}

func readResp(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	s := string(body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Request failed, %s, %v", resp.Status, s)
	} else {
		return s, nil
	}
}

func HttpGet(url *HttpUrl) (string, error) {
	c := new(http.Client)
	resp, err := c.Get(url.String())
	if err != nil {
		return "", err
	}
	return readResp(resp)
}

func HttpPostStr(url *HttpUrl, s string) (string, error) {
	buf := bytes.NewBuffer([]byte(s))
	c := new(http.Client)
	resp, err := c.Post(url.String(), MIME_TEXT, buf)
	if err != nil {
		return "", err
	}
	return readResp(resp)
}

func HttpPostData(url *HttpUrl, data interface{}) (string, error) {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		log.Error("Fail to post data during marshal, %v", err)
		return "", err
	}
	c := new(http.Client)
	resp, err := c.Post(url.String(), MIME_JSON, buf)
	if err != nil {
		return "", err
	}
	return readResp(resp)
}

func HttpReadRequestJsonBody(r *http.Request) ([]byte, error) {
	contentType := r.Header.Get("Content-Type")
	if contentType != MIME_JSON {
		err := fmt.Errorf("Expect MIME JSON, get %v", contentType)
		return nil, err
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	return body, nil
}

func HttpReadRequestTextBody(r *http.Request) (string, error) {
	contentType := r.Header.Get("Content-Type")
	if contentType != MIME_TEXT {
		err := fmt.Errorf("Expect MIME TEXT, get %v", contentType)
		return "", err
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	return string(body), nil
}

func HttpFitRequestInto(r *http.Request, v interface{}) error {
	buf, err := HttpReadRequestJsonBody(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf, v); err != nil {
		return err
	}
	return nil
}

func GetRequestAddr(r *http.Request) string {
	return r.RemoteAddr
}
