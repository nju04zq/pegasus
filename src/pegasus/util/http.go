package util

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

type HttpUrl struct {
	IP   string
	Port int
	Uri  string
}

func (url *HttpUrl) String() string {
	return fmt.Sprintf("http://%s:%d%s", url.IP, url.Port, url.Uri)
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

func HttpPostJsonData(url *HttpUrl, data *bytes.Buffer) (string, error) {
	c := new(http.Client)
	resp, err := c.Post(url.String(), "application/json", data)
	if err != nil {
		return "", err
	}
	return readResp(resp)
}
