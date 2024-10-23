package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type Server struct {
	Node     *maelstrom.Node
	Messages []int
}

var server Server

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", broadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)

	server = Server{n, make([]int, 0)}
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}

type broadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func broadcastHandler(msg maelstrom.Message) error {
	var body broadcastMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	server.Messages = append(server.Messages, body.Message)

	return server.Node.Reply(msg, map[string]string{
		"type": "broadcast_ok",
	})

}

func readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return server.Node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": server.Messages,
	})

}

type topologyMessage struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func topologyHandler(msg maelstrom.Message) error {
	var body topologyMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return server.Node.Reply(msg, map[string]string{
		"type": "topology_ok",
	})

}