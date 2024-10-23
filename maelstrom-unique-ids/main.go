package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type GenerateResponse struct {
	Type string `json:"type"`
	Id   string `json:"id"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {

		// TODO: actually implement some id generation
		return n.Reply(msg, &GenerateResponse{
			Type: "generate_ok",
			Id:   "whatever",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
