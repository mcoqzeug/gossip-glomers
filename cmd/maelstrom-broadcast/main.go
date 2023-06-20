package main

import (
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// ./maelstrom test -w broadcast --bin ./bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition
	done := make(chan struct{})
	b := NewBroadcaster(n, done)
	defer func() {
		done <- struct{}{}
	}()

	n.Handle("broadcast", b.Broadcast)
	n.Handle("read", b.Read)
	n.Handle("topology", b.Topology)
	n.Handle("gossip", b.Gossip)

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
