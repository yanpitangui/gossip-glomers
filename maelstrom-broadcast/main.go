package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"slices"
	"sync"
)

type Server struct {
	Node     *maelstrom.Node
	Messages []int
	Topology topology
	Mutex    *sync.Mutex
}

var server Server

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", broadcastHandler)
	n.Handle("receive_broadcast", receiveBroadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)

	server = Server{n, make([]int, 0), topology{}, &sync.Mutex{}}
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

	server.Mutex.Lock()
	if slices.Contains(server.Messages, body.Message) {
		server.Mutex.Unlock()
		return server.Node.Reply(msg, map[string]string{
			"type": "broadcast_ok",
		})
	}

	server.Messages = append(server.Messages, body.Message)
	server.Mutex.Unlock()

	for _, recipient := range server.Node.NodeIDs() {
		if err := server.Node.Send(recipient,
			receiveBroadcast{
				Type:    "receive_broadcast",
				Message: body.Message,
			}); err != nil {
			return err
		}
	}

	return server.Node.Reply(msg, map[string]string{
		"type": "broadcast_ok",
	})
}

type receiveBroadcast struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func receiveBroadcastHandler(msg maelstrom.Message) error {

	var body receiveBroadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	server.Mutex.Lock()
	if slices.Contains(server.Messages, body.Message) {
		server.Mutex.Unlock()
		return nil
	}

	server.Messages = append(server.Messages, body.Message)
	server.Mutex.Unlock()

	return nil
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
	Type     string   `json:"type"`
	Topology topology `json:"topology"`
}

type topology map[string][]string

func topologyHandler(msg maelstrom.Message) error {
	var body topologyMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return server.Node.Reply(msg, map[string]string{
		"type": "topology_ok",
	})

}
