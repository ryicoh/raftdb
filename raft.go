package raftdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

type (
	RaftNode struct {
		mu          sync.Mutex
		raft        *raft.Raft
		logStore    raft.LogStore
		stableStore raft.StableStore
		snapStore   raft.SnapshotStore
		dataStore   DataStore

		applyTimeout time.Duration
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

func NewRaftNode(
	name, datadir, peerAddress string,
	logStore raft.LogStore, stableStore raft.StableStore,
	dataStore DataStore, debug bool, logger hclog.Logger) (*RaftNode, error) {
	logWriter := logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true})
	snapStore, err := raft.NewFileSnapshotStore(path.Join(datadir, "snapshot"), 3, logWriter)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", peerAddress)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(peerAddress, addr, 10, 5*time.Second, logWriter)
	if err != nil {
		return nil, err
	}

	node := &RaftNode{
		dataStore:    dataStore,
		applyTimeout: 5 * time.Second,
	}
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(name)
	if !debug {
		conf.LogLevel = "INFO"
	}

	rf, err := raft.NewRaft(conf, node, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, err
	}

	node.raft = rf
	node.logStore = logStore
	node.stableStore = stableStore
	node.snapStore = snapStore

	return node, nil
}

func (r *RaftNode) WaitUntilJoinCluster(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		if r.raft.Leader() != "" {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(time.Second * 1)
		}
	}
}

func (r *RaftNode) Start(cluster string) error {
	exists, err := raft.HasExistingState(r.logStore, r.stableStore, r.snapStore)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	endpoints := strings.Split(cluster, ",")
	if len(endpoints) == 0 {
		log.Fatalf("少なくとも１つ以上のノードが必要です")
	}
	servers := make([]raft.Server, 0)
	for _, endpoint := range endpoints {
		parts := strings.Split(endpoint, "=")
		if len(parts) != 2 {
			log.Fatalf("cluster フラグの形式が間違っています")
		}
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(parts[0]),
			Address:  raft.ServerAddress(parts[1]),
		})
	}
	if err := r.raft.BootstrapCluster(raft.Configuration{
		Servers: servers,
	}).Error(); err != nil {
		return err
	}
	return nil
}

func (r *RaftNode) Shutdown() error {
	if err := r.raft.LeadershipTransfer().Error(); err != nil {
		return err
	}
	return r.raft.Shutdown().Error()
}

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

	if err := r.raft.Apply(data, r.applyTimeout).Error(); err != nil {
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
		err := r.dataStore.Set(cmd.Key, cmd.Value)
		return &applyResponse{error: err}
	case commandDel:
		err := r.dataStore.Delete(cmd.Key)
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
