package main

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/binary"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"slices"
	"time"
)

type GenerateResponse struct {
	Type string `json:"type"`
	Id   string `json:"id"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {

		id, err := Generate()
		if err != nil {
			return err
		}

		return n.Reply(msg, &GenerateResponse{
			Type: "generate_ok",
			Id:   id,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func Generate() (string, error) {
	nano := time.Now().UnixNano()
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, nano)
	timePart := buf[:n]
	slices.Reverse(timePart)
	randBytes := make([]byte, 6)
	n, err := rand.Read(randBytes)
	randPart := randBytes[:n]

	if err != nil {
		return "", err
	}
	return base32.StdEncoding.EncodeToString(append(timePart, randPart...)), nil
}
