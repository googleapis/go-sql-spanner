package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"spannerlib/socket-server/server"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Missing server address\n")
	}
	name := os.Args[1]
	tp := "unix"
	if len(os.Args) > 2 {
		tp = os.Args[2]
	}
	//
	if tp == "unix" {
		defer func() { _ = os.Remove(name) }()
		// Set up a channel to listen for OS signals that terminate the process,
		// so we can clean up the temp file in those cases as well.
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			// Wait for a signal.
			<-sigs
			// Delete the temp file.
			_ = os.Remove(name)
			os.Exit(0)
		}()
	}

	listener, err := net.Listen(tp, name)
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}
	defer func() { _ = listener.Close() }()
	log.Printf("Starting server on %s\n", listener.Addr().String())
	s, err := server.CreateServer()
	if err != nil {
		log.Fatalf("failed to create server: %v\n", err)
	}
	if err := s.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v\n", err)
	}
}
