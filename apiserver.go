package raftdb

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/hashicorp/raft"
)

type APIServer struct {
	*http.ServeMux
	*RaftNode
	DataStore
	raft.LogStore
}

// APIサーバは http.Handler を実装
var _apiServer http.Handler = &APIServer{}

func NewAPIServer(rf *RaftNode, dataStore DataStore, logStore raft.LogStore) *APIServer {
	mux := http.NewServeMux()
	api := &APIServer{
		ServeMux:  mux,
		RaftNode:  rf,
		DataStore: dataStore,
		LogStore:  logStore,
	}

	mux.HandleFunc("/index", api.handleIndex)
	mux.HandleFunc("/key/", api.handleKey)

	return api
}

func (a *APIServer) handleIndex(rw http.ResponseWriter, r *http.Request) {
	index, err := a.LastIndex()
	if err != nil {
		http.Error(rw, "インデックスが見つかりません", http.StatusNotFound)
		return
	}

	rw.Write([]byte(strconv.FormatUint(index, 10)))
}

func (a *APIServer) handleKey(rw http.ResponseWriter, r *http.Request) {
	key := []byte(strings.TrimPrefix(r.URL.Path, "/key/"))
	if len(key) == 0 {
		http.Error(rw, "キーが取得できませんでした", http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodGet {
		val, err := a.Get(key)
		if err != nil {
			http.Error(rw, "データの取得に失敗しました", http.StatusInternalServerError)
			return
		}
		if val == nil {
			http.Error(rw, "データが存在しません", http.StatusNotFound)
			return
		}
		rw.Write(val)
		return
	}

	if a.raft.State() != raft.Leader {
		http.Error(rw, "リーダーではないため、リクエストを処理できません", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost, http.MethodPut:
		value, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(rw, "パスパラメータを取得できません", http.StatusBadRequest)
			return
		}

		if err := a.ApplyPut(key, value); err != nil {
			http.Error(rw, fmt.Sprintf("書き込みに失敗しました: %v", err), http.StatusInternalServerError)
			return
		}
	case http.MethodDelete:
		if err := a.ApplyDelete(key); err != nil {
			http.Error(rw, fmt.Sprintf("書き込みに失敗しました: %v", err), http.StatusInternalServerError)
			return
		}
	}

	rw.Write([]byte("ok"))
}
