package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/ryicoh/raftdb"
)

func main() {
	initalizeFlags()

	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// コマンドのフラグを利用するための変数
var (
	//Raftのクラスタ内で利用する一意な名前
	// 例: node1
	name string

	// Raftで通信するためのサーバアドレス
	// 例: localhost:8081
	peerAddress string

	// クライアントからのリクエストを受け付けるサーバアドレス
	// 例: localhost:8091
	clientAddress string

	// Raftのクラスタを設定するための`name`と`peer-address`のペア
	// 例: node1=localhost:8081,node2=localhost:8082,node3=localhost:8083
	cluster string

	// データを保存するためのディレクトリ
	// 例: /tmp/raftdb
	datadir string

	// デバッグモード
	debug bool

	// ロガー
	logger hclog.Logger
)

// フラグを初期化
func initalizeFlags() {
	flag.StringVar(&name, "name", "default", "Raftのクラスタ内で利用する一意な名前")
	flag.StringVar(&peerAddress, "peer-address", "localhost:8081", "Raftで通信するためのサーバアドレス")
	flag.StringVar(&clientAddress, "client-address", "localhost:8091", "クライアントからのリクエストを受け付けるサーバアドレス")
	flag.StringVar(&cluster, "cluster", "default=localhost:8081", "Raftのクラスタを設定するための`name`と`peer-address`のペア")
	flag.StringVar(&datadir, "datadir", "/tmp/raftdb", "データを保存するためのディレクトリ")
	flag.BoolVar(&debug, "debug", false, "デバッグモード")

	flag.Parse()

	level := "debug"
	if !debug {
		level = "info"
	}
	logger = hclog.New(&hclog.LoggerOptions{
		Name:  "raftdb",
		Level: hclog.LevelFromString(level),
	})
}

func newStores() (raft.LogStore, raft.StableStore, raftdb.DataStore, error) {
	if _, err := os.Stat(datadir); os.IsNotExist(err) {
		if err := os.Mkdir(datadir, 0700); err != nil {
			return nil, nil, nil, err
		}
	}

	logStore, err := raftdb.NewRocksDBStore(path.Join(datadir, "log"), logger.Named("logstore"))
	if err != nil {
		return nil, nil, nil, err
	}
	stableStore, err := raftdb.NewRocksDBStore(path.Join(datadir, "stable"), logger.Named("stablestore"))
	if err != nil {
		return nil, nil, nil, err
	}
	dataStore, err := raftdb.NewRocksDBStore(path.Join(datadir, "data"), logger.Named("datastore"))
	if err != nil {
		return nil, nil, nil, err
	}

	return logStore, stableStore, dataStore, nil
}

func run() error {
	logStore, stableStore, dataStore, err := newStores()
	if err != nil {
		return err
	}

	node, err := raftdb.NewRaftNode(name, datadir, peerAddress, logStore, stableStore, dataStore, debug, logger)
	if err != nil {
		return err
	}

	if err := node.Start(cluster); err != nil {
		return err
	}
	if err := node.WaitUntilJoinCluster(30 * time.Second); err != nil {
		return err
	}

	apiserver := raftdb.NewAPIServer(node, dataStore, logStore)
	server := &http.Server{
		Addr:    clientAddress,
		Handler: apiserver,
	}

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		logger.Info("APIサーバを起動")
		if err := server.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal)
	defer close(stop)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-stop:
		logger.Info("停止シグナルを受け取りました")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			return err
		}
		return node.Shutdown()

	case err := <-errCh:
		return err
	}
}
