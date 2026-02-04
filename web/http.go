package web

import "net/http"

func init() {
	server := http.NewServeMux()
	server.HandleFunc("/gmq", func(writer http.ResponseWriter, request *http.Request) {

	})
	go func() {
		_ = http.ListenAndServe("", server)
	}()
}
