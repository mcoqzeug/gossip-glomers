package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// ./maelstrom test -w echo --bin ./bin/maelstrom-echo --node-count 1 --time-limit 10
	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "echo_ok"

		return n.Reply(msg, body)
	})

	// ./maelstrom test -w unique-ids --bin ./bin/maelstrom-echo --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
	n.Handle("generate", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   uuid.NewString(),
		})
	})

	// ./maelstrom test -w g-counter --bin ./bin/maelstrom-echo --node-count 3 --rate 100 --time-limit 20 --nemesis partition
	counter := NewCounter(n)

	n.Handle("read", counter.Read)
	n.Handle("add", counter.Add)

	// ./maelstrom test -w kafka --bin ./bin/maelstrom-echo --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
	k := NewKafka(n)

	n.Handle("send", k.Send)
	n.Handle("poll", k.Poll)
	n.Handle("commit_offsets", k.CommitOffsets)
	n.Handle("list_committed_offsets", k.ListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
