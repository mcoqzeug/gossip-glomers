package main

import (
	"encoding/json"
	"testing"
)

func TestJson(t *testing.T) {
	req := PollResponse{
		"poll_ok",
		map[string][][]int{
			"k1": {{1000, 9}, {1001, 5}, {1002, 15}},
			"k2": {{2000, 7}, {2001, 2}},
		},
	}

	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(b))

	req2 := &PollResponse{}
	if err = json.Unmarshal(b, req2); err != nil {
		t.Fatal(err)
	}

	t.Log(req2)
}
