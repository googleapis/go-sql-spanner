package server

import (
	"bufio"
	"log"
	"net"

	"spannerlib/socket-server/message"
)

type ConnectionHandler struct {
	server *Server
	conn   net.Conn

	reader *bufio.Reader
	writer *bufio.Writer

	poolId int64
	connId int64

	handler *messageHandler
}

func (c *ConnectionHandler) handleConnection() {
	for {
		msg, err := message.ReadMessage(c.reader)
		if err != nil {
			log.Printf("Error reading message from client: %v", err)
			break
		}
		log.Printf("Received message: %s\n", msg)
		if err := msg.Handle(c.handler); err != nil {
			log.Printf("error handling message: %v\n", err)
			resp := message.CreateStatusMessage(err)
			if err := resp.Write(c.writer); err != nil {
				log.Printf("error writing response: %v\n", err)
				break
			}
		}
		if err := c.writer.Flush(); err != nil {
			log.Printf("error flushing response: %v\n", err)
			break
		}
	}
	_ = c.conn.Close()
}
