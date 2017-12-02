package route

import (
	"net/http"
	"pegasus/log"

	"github.com/gorilla/mux"
)

type RouteHandler func(http.ResponseWriter, *http.Request)

type Route struct {
	Name    string
	Method  string
	Path    string
	Handler RouteHandler
}

var routes = []*Route{}

func RegisterRoute(r *Route) {
	routes = append(routes, r)
}

func BuildRouter() *mux.Router {
	r := mux.NewRouter()
	for _, route := range routes {
		log.Info("Add route %q", route.Name)
		r.HandleFunc(route.Path, route.Handler).Methods(route.Method)
	}
	return r
}
