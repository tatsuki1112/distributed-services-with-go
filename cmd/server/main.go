package main

import (
	"github.com/tatsuki1112/distributed-services-with-go/internal/server"
	"log"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
