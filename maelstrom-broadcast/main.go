package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"maps"
	"slices"
	"sync"
	"time"
)

type Server struct {
	Node     *maelstrom.Node
	Messages map[int]struct{}
	Topology topology
	Mutex    *sync.RWMutex
}

var server Server

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", broadcastHandler)
	n.Handle("receive_broadcast", receiveBroadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)

	server = Server{n, make(map[int]struct{}, 0), topology{}, &sync.RWMutex{}}
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

	if checkMessageExists(body.Message) {
		return server.Node.Reply(msg, map[string]string{
			"type": "broadcast_ok",
		})
	}

	storeMessage(body.Message)

	for _, recipient := range server.Node.NodeIDs() {
		go func() {
			var acked bool
			for !acked {
				_ = server.Node.RPC(recipient,
					receiveBroadcast{
						Type:    "receive_broadcast",
						Message: body.Message,
					}, func(msg maelstrom.Message) error {
						acked = true
						return nil
					})
			}
			time.Sleep(500 * time.Millisecond)

		}()
	}

	return server.Node.Reply(msg, map[string]string{
		"type": "broadcast_ok",
	})
}

type receiveBroadcast struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func checkMessageExists(msg int) bool {
	server.Mutex.RLock()
	defer server.Mutex.RUnlock()
	_, found := server.Messages[msg]
	return found
}

func storeMessage(msg int) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	server.Messages[msg] = struct{}{}
}

func receiveBroadcastHandler(msg maelstrom.Message) error {

	var body receiveBroadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if checkMessageExists(body.Message) {
		return nil
	}

	storeMessage(body.Message)

	return nil
}

func readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	server.Mutex.RLock()
	defer server.Mutex.RUnlock()
	return server.Node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": slices.Collect(maps.Keys(server.Messages)),
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
