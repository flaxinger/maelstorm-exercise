package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var memory map[float64]bool
var mu sync.Mutex

func getValue(m map[string]any, key string) float64 {
	number, _ := m[key].(float64)
	return number
}

func write(value float64) {
	mu.Lock()
	memory[value] = true
	mu.Unlock()
}

func main() {
	n := maelstrom.NewNode()
	memory = make(map[float64]bool)

	n.Handle("write", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		number := getValue(body, "value")
		write(number)
		return n.Reply(msg, body)
	})
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		number := getValue(body, "message")
		response := map[string]string{
			"type": "broadcast_ok",
		}

		messageToSend := map[string]any{
			"type":  "write",
			"value": number,
		}
		write(number)

		for _, id := range n.NodeIDs() {
			if id == n.ID() {
				continue
			}

			err := n.Send(id, messageToSend)
			if err != nil {
				log.Fatal("wtf")
			}
		}

		return n.Reply(msg, response)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		arr := make([]int, 0)
		for k := range memory {
			arr = append(arr, int(k))
		}
		body["messages"] = arr

		return n.Reply(msg, body)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delete(body, "topology")
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
