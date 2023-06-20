package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendRequest struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type SendResponse struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type PollRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	Type string             `json:"type"`
	Msgs map[string][][]int `json:"msgs"`
}

type CommitOffsetsRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type ListCommittedOffsetsRequest struct {
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type Kafka struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func NewKafka(n *maelstrom.Node) *Kafka {
	return &Kafka{
		n:  n,
		kv: maelstrom.NewSeqKV(n),
	}
}

func (k *Kafka) Send(msg maelstrom.Message) error {
	req := &SendRequest{}
	if err := json.Unmarshal(msg.Body, req); err != nil {
		return err
	}

	ctx := context.Background()
	offset := 0

	for {
		var logs [][]int

		err := k.kv.ReadInto(ctx, req.Key, &logs)
		readErr := &maelstrom.RPCError{}

		if err != nil && !(errors.As(err, &readErr) && readErr.Code == maelstrom.KeyDoesNotExist) {
			return err
		}

		if len(logs) > 0 {
			offset = logs[len(logs)-1][0] + 1
		}

		newLogs := append(logs, []int{offset, req.Msg})
		err = k.kv.CompareAndSwap(ctx, req.Key, logs, newLogs, true)

		casErr := &maelstrom.RPCError{}
		if errors.As(err, &casErr) && casErr.Code == maelstrom.PreconditionFailed {
			continue
		}

		if err != nil {
			return err
		}

		return k.n.Reply(msg, SendResponse{
			Type:   "send_ok",
			Offset: offset,
		})
	}
}

func (k *Kafka) Poll(msg maelstrom.Message) error {
	req := &PollRequest{}
	if err := json.Unmarshal(msg.Body, req); err != nil {
		return err
	}

	ctx := context.Background()
	msgs := make(map[string][][]int)

	for key, offset := range req.Offsets {
		var logs [][]int

		err := k.kv.ReadInto(ctx, key, &logs)
		readErr := &maelstrom.RPCError{}

		if err != nil && !(errors.As(err, &readErr) && readErr.Code == maelstrom.KeyDoesNotExist) {
			return err
		}

		for i, l := range logs {
			if l[0] < offset {
				continue
			}

			msgs[key] = logs[i:]

			break
		}
	}

	return k.n.Reply(msg, PollResponse{
		Type: "poll_ok",
		Msgs: msgs,
	})
}

func (k *Kafka) CommitOffsets(msg maelstrom.Message) error {
	req := &CommitOffsetsRequest{}
	if err := json.Unmarshal(msg.Body, req); err != nil {
		return err
	}

	ctx := context.Background()

	for key, offset := range req.Offsets {
		if err := k.kv.Write(ctx, getCommitKey(key), offset); err != nil {
			return err
		}
	}

	return k.n.Reply(msg, map[string]string{"type": "commit_offsets_ok"})
}

func (k *Kafka) ListCommittedOffsets(msg maelstrom.Message) error {
	req := &ListCommittedOffsetsRequest{}
	err := json.Unmarshal(msg.Body, req)
	if err != nil {
		return err
	}

	ctx := context.Background()
	res := make(map[string]int, len(req.Keys))

	for _, key := range req.Keys {
		res[key], err = k.kv.ReadInt(ctx, getCommitKey(key))
		if err != nil {
			return err
		}
	}

	return k.n.Reply(msg, ListCommittedOffsetsResponse{
		Type:    "list_committed_offsets_ok",
		Offsets: res,
	})
}

func getCommitKey(key string) string {
	return fmt.Sprintf("%s_commit", key)
}
