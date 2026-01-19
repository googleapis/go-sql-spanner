package server

import (
	"bufio"
	"net"
	"sync"
)

func CreateServer() (*Server, error) {
	return &Server{
		handlers: make([]*ConnectionHandler, 0),
		pools:    &sync.Map{},
	}, nil
}

type Server struct {
	listener net.Listener
	handlers []*ConnectionHandler

	pools *sync.Map
}

func (s *Server) GracefulStop() {
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
}

func (s *Server) Serve(listener net.Listener) error {
	s.listener = listener
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		connectionHandler := &ConnectionHandler{
			server: s,
			conn:   conn,
			reader: bufio.NewReader(conn),
			writer: bufio.NewWriter(conn),
		}
		connectionHandler.handler = &messageHandler{
			conn: connectionHandler,
		}
		s.handlers = append(s.handlers, connectionHandler)
		go connectionHandler.handleConnection()
	}
}
