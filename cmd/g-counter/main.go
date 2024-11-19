package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var kv *maelstrom.KV

const (
	V = "value"
)

func write(value int) error {
	current, err := kv.ReadInt(context.Background(), V)
	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			err = kv.CompareAndSwap(context.Background(), V, nil, value, true)
		} else {
			return err
		}
	} else {
		err = kv.CompareAndSwap(context.Background(), V, current, current+value, false)
	}

	if err != nil {
		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			return write(value)
		}
	}
	return err
}

func main() {
	n := maelstrom.NewNode()
	kv = maelstrom.NewSeqKV(n)
	n.Handle("add", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := write(int(body["delta"].(float64))); err != nil {
			return err
		}

		response := map[string]string{
			"type": "add_ok",
		}
		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		current, _ := kv.ReadInt(context.Background(), V)
		response := map[string]any{
			"type":  "read_ok",
			"value": current,
		}
		return n.Reply(msg, response)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
