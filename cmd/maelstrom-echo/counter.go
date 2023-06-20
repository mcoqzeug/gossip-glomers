package main

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddRequest struct {
	Delta int `json:"delta"`
}

type Counter struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func NewCounter(n *maelstrom.Node) *Counter {
	return &Counter{
		n:  n,
		kv: maelstrom.NewSeqKV(n),
	}
}

const (
	intKey    = "counter"
	uniqueKey = "unique"
)

func (c *Counter) Add(msg maelstrom.Message) error {
	req := &AddRequest{}
	if err := json.Unmarshal(msg.Body, req); err != nil {
		return err
	}

	ctx := context.Background()

	for {
		val, err := c.kv.ReadInt(ctx, intKey)
		readErr := &maelstrom.RPCError{}

		if err != nil && !(errors.As(err, &readErr) && readErr.Code == maelstrom.KeyDoesNotExist) {
			return err
		}

		err = c.kv.CompareAndSwap(ctx, intKey, val, val+req.Delta, true)
		casErr := &maelstrom.RPCError{}

		if errors.As(err, &casErr) && casErr.Code == maelstrom.PreconditionFailed {
			continue
		}

		if err != nil {
			return err
		}

		return c.n.Reply(msg, map[string]any{"type": "add_ok"})
	}
}

func (c *Counter) Read(msg maelstrom.Message) error {
	ctx := context.Background()

	// Write a unique value to the uniqueKey to ensure that we are reading the latest value
	// See https://github.com/jepsen-io/maelstrom/issues/39
	if err := c.kv.Write(ctx, uniqueKey, uuid.NewString()); err != nil {
		return err
	}

	val, err := c.kv.ReadInt(ctx, intKey)
	if err != nil {
		return err
	}

	return c.n.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": val,
	})
}
