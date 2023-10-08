package main

import (
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastRequest struct {
	Message int64 `json:"message"`
}

type ReadResponse struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

type TopologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

type GossipRequest struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

type GossipResponse struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

type Broadcaster struct {
	n            *maelstrom.Node
	neighborKeys []string

	mu        sync.Mutex
	store     map[int64]struct{}            // set of msgs we have recieved
	neighbors map[string]map[int64]struct{} // neighbor -> set of msg we think they have received
}

func NewBroadcaster(n *maelstrom.Node, done <-chan struct{}) *Broadcaster {
	b := &Broadcaster{
		n:            n,
		neighborKeys: make([]string, 0),
		store:        make(map[int64]struct{}),
		neighbors:    make(map[string]map[int64]struct{}),
	}

	// sync wiht neighbors periodically
	// tell neighbors what we think they don't have

	ticker := time.NewTicker(500 * time.Millisecond)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for _, neighbor := range b.neighborKeys {
					b.mu.Lock()
					msgSet := setMinus(b.store, b.neighbors[neighbor])
					b.mu.Unlock()

					msgs := set2List(msgSet)
					if len(msgs) == 0 {
						continue
					}

					b.n.RPC(
						neighbor,
						GossipRequest{
							Type:     "gossip",
							Messages: msgs,
						},
						func(msg maelstrom.Message) error {
							res := &GossipResponse{}
							if err := json.Unmarshal(msg.Body, res); err != nil {
								return err
							}

							resMsgSet := list2Set(res.Messages)

							b.mu.Lock()

							b.neighbors[msg.Src] = setUnion3(b.neighbors[msg.Src], resMsgSet, msgSet)
							b.store = setUnion(b.store, resMsgSet)

							b.mu.Unlock()

							return nil
						},
					)
				}
			}
		}
	}()

	return b
}

// Gossip adds msgs to our store and tell the sender what we think they don't have
func (b *Broadcaster) Gossip(msg maelstrom.Message) error {
	req := &GossipRequest{}
	if err := json.Unmarshal(msg.Body, req); err != nil {
		return err
	}

	reqMsgSet := list2Set(req.Messages)

	b.mu.Lock()

	b.neighbors[msg.Src] = setUnion(b.neighbors[msg.Src], reqMsgSet)
	b.store = setUnion(b.store, reqMsgSet)
	msgSet := setMinus(b.store, b.neighbors[msg.Src])

	b.mu.Unlock()

	return b.n.Reply(msg, GossipResponse{
		Type:     "gossip_ok",
		Messages: set2List(msgSet),
	})
}

func (b *Broadcaster) Broadcast(msg maelstrom.Message) error {
	req := &BroadcastRequest{}
	if err := json.Unmarshal(msg.Body, req); err != nil {
		return err
	}

	b.mu.Lock()
	b.store[req.Message] = struct{}{}
	b.mu.Unlock()

	return b.n.Reply(msg, map[string]string{"type": "broadcast_ok"})
}

func (b *Broadcaster) Read(msg maelstrom.Message) error {
	b.mu.Lock()
	msgs := set2List(b.store)
	b.mu.Unlock()

	return b.n.Reply(msg, ReadResponse{
		Type:     "read_ok",
		Messages: msgs,
	})
}

// Topology describes the neighbors of each node
func (b *Broadcaster) Topology(msg maelstrom.Message) error {
	req := &TopologyRequest{}
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	for _, neighbor := range req.Topology[b.n.ID()] {
		b.neighborKeys = append(b.neighborKeys, neighbor)
		b.neighbors[neighbor] = make(map[int64]struct{})
	}

	return b.n.Reply(msg, map[string]string{"type": "topology_ok"})
}
