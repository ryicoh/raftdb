package raftdb

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type (
	RaftNode struct {
		mu           sync.Mutex
		raft         *raft.Raft
		store        DataStore
		ApplyTimeout time.Duration
	}

	commandKind int
	command     struct {
		Kind  commandKind `json:"kind"`
		Key   []byte      `json:"key"`
		Value []byte      `json:"value"`
	}

	applyResponse struct {
		error error
	}
)

// RaftNode は raft.FSM を実装
var _raftNode raft.FSM = &RaftNode{}

const (
	commandPut commandKind = iota + 1
	commandDel
)

// ApplyPut, ApplyDel, apply はログ複製の送信側処理
func (r *RaftNode) ApplyPut(key, value []byte) error {
	return r.apply(&command{
		Kind:  commandPut,
		Key:   key,
		Value: value,
	})
}

func (r *RaftNode) ApplyDelete(key []byte) error {
	return r.apply(&command{
		Kind: commandDel,
		Key:  key,
	})
}

func (r *RaftNode) apply(cmd *command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	if err := r.raft.Apply(data, r.ApplyTimeout).Error(); err != nil {
		return err
	}
	return nil
}

// Apply はログ複製の受信側処理
func (r *RaftNode) Apply(log *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return &applyResponse{error: err}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	switch cmd.Kind {
	case commandPut:
		err := r.store.Set(cmd.Key, cmd.Value)
		return &applyResponse{error: err}
	case commandDel:
		err := r.store.Delete(cmd.Key)
		return &applyResponse{error: err}
	default:
		return &applyResponse{error: fmt.Errorf("サポートされていない種類のコマンドです: %v", cmd.Kind)}
	}
}

func (r *RaftNode) Snapshot() (raft.FSMSnapshot, error) {
	panic("not implemented") // TODO: Implement
}
func (r *RaftNode) Restore(_ io.ReadCloser) error {
	panic("not implemented") // TODO: Implement
}
