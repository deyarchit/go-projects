package main

import (
	"log"

	"github.com/deyboy90/go-projects/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
