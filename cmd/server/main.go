package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

var port *int

func init() {
	port = flag.Int("port", 6969, "Port to run the server")
}

func main() {
	flag.Parse()
	mux := http.NewServeMux()
	s := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	log.Fatal(s.ListenAndServe())
}
