package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"pegasus/log"
	"pegasus/route"
)

func marshalData(data interface{}) ([]byte, error) {
	s, ok := data.(string)
	if ok {
		return []byte(s), nil
	}
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		log.Error("Fail on json marshal, %v", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

func FmtResp(w http.ResponseWriter, err error, data interface{}) {
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, err.Error())
	} else {
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		buf, err := marshalData(data)
		if err != nil {
			FmtResp(w, err, nil)
		}
		_, err = w.Write(buf)
		if err != nil {
			log.Error("Fail to write resp data, %v", err)
		}
	}
}

type serverHandler struct {
	handler http.Handler
}

func (h *serverHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Info("Remote %s, method %s, access %s", r.RemoteAddr, r.Method, r.RequestURI)
	h.handler.ServeHTTP(w, r)
}

type Server struct {
	listener net.Listener
}

func (s *Server) Listen(ip string) (err error) {
	addr := fmt.Sprintf("%s:", ip)
	s.listener, err = net.Listen("tcp", addr)
	return
}

func (s *Server) GetListenAddr() string {
	return s.listener.Addr().String()
}

func (s *Server) Serve() error {
	r := route.BuildRouter()
	handler := &serverHandler{
		handler: r,
	}
	return http.Serve(s.listener, handler)
}

func (s *Server) ListenAndServe(listenPort int) error {
	r := route.BuildRouter()
	handler := &serverHandler{
		handler: r,
	}
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", listenPort),
		Handler: handler,
	}
	return httpServer.ListenAndServe()
}
