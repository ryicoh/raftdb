package raftdb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/hashicorp/raft"
)

type APIServer struct {
	*RaftNode
	DataStore
}

// APIサーバは http.Handler を実装
var _apiServer http.Handler = &APIServer{}

func (a *APIServer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	key, err := a.decodePath(r.URL.Path)
	if err != nil {
		http.Error(rw, "パスパラメータを取得できません", http.StatusBadRequest)
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

func (a *APIServer) decodePath(path string) ([]byte, error) {
	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return nil, errors.New("パスからキーを取り出せませんでした")
	}
	if parts[0] != "key" {
		return nil, errors.New("パスは /key/:key である必要があります")
	}

	return []byte(parts[1]), nil
}
