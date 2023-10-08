package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// TOOD:
// https://community.fly.io/t/gossip-glomers-challenge-6/11960
// https://community.fly.io/t/hints-thoughts-on-approaches-to-challenge-6-how-to-implement-g0-and-make-maelstrom-happy/12079
// https://community.fly.io/t/challenge-6-maelstrom-does-not-detect-g0-even-when-it-can-how-to-abort-txs-for-g1a/11807
// https://github.com/ept/hermitage

type TxnRequest struct {
	// Txn:
	// [
	//   ["r", 1.0, nil],
	//   ["w", 1.0, 6.0],
	//   ["w", 2.0, 9.0]
	// ]
	Txn [][3]any `json:"txn"`
}

type TxnResponse struct {
	Type string   `json:"type"`
	Txn  [][3]any `json:"txn"`
}

type TxnKV struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func NewTxnKV(n *maelstrom.Node) *TxnKV {
	return &TxnKV{
		n:  n,
		kv: maelstrom.NewLinKV(n),
	}
}

func (t *TxnKV) Txn(msg maelstrom.Message) error {
	req := &TxnRequest{}
	if err := json.Unmarshal(msg.Body, req); err != nil {
		return err
	}

	ctx := context.Background()

	for i := range req.Txn {
		o, ok := req.Txn[i][0].(string)
		if !ok {
			log.Fatalf("invalid req.Txn[%d][0]: %v %T", i, req.Txn[i][0], req.Txn[i][0])
		}

		intKey, ok := req.Txn[i][1].(float64)
		if !ok {
			log.Fatalf("invalid req.Txn[%d][1]: %v %T", i, req.Txn[i][1], req.Txn[i][1])
		}

		key := strconv.FormatInt(int64(intKey), 10)

		switch o {
		case "r":
			val, err := t.handleRead(ctx, key)
			if err != nil {
				return err
			}

			req.Txn[i][2] = val
		case "w":
			val, ok := req.Txn[i][2].(float64)
			if !ok {
				log.Fatalf("invalid req.Txn[%d][2]: %v %T", i, req.Txn[i][2], req.Txn[i][2])
			}

			return t.handleWrite(ctx, key, int(val))
		}
	}

	return t.n.Reply(msg, TxnResponse{
		Type: "txn_ok",
		Txn:  req.Txn,
	})
}

func (t *TxnKV) handleRead(ctx context.Context, key string) (int, error) {
	val, err := t.kv.ReadInt(ctx, key)
	readErr := &maelstrom.RPCError{}

	if err != nil && !(errors.As(err, &readErr) && readErr.Code == maelstrom.KeyDoesNotExist) {
		return 0, err
	}

	return val, nil
}

func (t *TxnKV) handleWrite(ctx context.Context, key string, val int) error {
	return t.kv.Write(ctx, key, val)
}
