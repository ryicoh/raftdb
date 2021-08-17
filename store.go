package raftdb

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/flier/gorocksdb"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// ストレージエンジンには、rocksdbを利用
// https://github.com/facebook/rocksdb
// Goで利用する場合は、gorocksdbを使う
// 本家のgorocksdbはv6.15以上に対応していないため、
// Pull Requestを出していた http://github.com/flier/gorocksdb を利用
type RocksDBStore struct {
	db     *gorocksdb.DB
	logger hclog.Logger
}

// データ保存用の独自のストア
type DataStore interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, val []byte) error
	Delete(key []byte) error
}

// RocksDBStoreが以下のインターフェイスを実装していることを保証する
// * raft.LogStore
// * raft.StableStore
// * DataStore
var (
	_logStore    raft.LogStore    = &RocksDBStore{}
	_stableStore raft.StableStore = &RocksDBStore{}
	_dataStore   DataStore        = &RocksDBStore{}
)

// 読み書きのオプション
// オプションを指定しない場合は、わざわざインスタンスを作る必要はないため、
// こちらに変数を定義しておく
var (
	defaultReadOptions  = gorocksdb.NewDefaultReadOptions()
	defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()
)

func NewRocksDBStore(name string, logger hclog.Logger) (*RocksDBStore, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		return nil, err
	}

	return &RocksDBStore{db, logger}, nil
}

// Set, Get, GetUint64, SetUint64 は、raft.StableStore を実装
func (r *RocksDBStore) Set(key []byte, val []byte) error {
	r.logger.Debug("Set", "key", string(key), "val", string(val))

	return r.db.Put(defaultWriteOptions, key, val)
}

func (r *RocksDBStore) Get(key []byte) (val []byte, err error) {
	defer func() {
		r.logger.Debug("Get", "key", string(key), "val", string(val))
	}()

	slice, err := r.db.Get(defaultReadOptions, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	if slice.Data() == nil {
		return nil, err
	}

	val = make([]byte, len(slice.Data()))
	copy(val, slice.Data())
	return
}

func (r *RocksDBStore) Delete(key []byte) error {
	r.logger.Debug("Delete", "key", string(key))

	return r.db.Delete(defaultWriteOptions, key)
}

func (r *RocksDBStore) SetUint64(key []byte, val uint64) error {
	r.logger.Debug("SetUint64", "key", string(key), "val", val)

	return r.Set(key, []byte(strconv.FormatUint(val, 10)))
}

func (r *RocksDBStore) GetUint64(key []byte) (u64 uint64, err error) {
	defer func() {
		r.logger.Debug("GetUint64", "key", string(key), "val", u64)
	}()

	val, err := r.Get(key)
	if err != nil {
		return 0, err
	}

	if val == nil {
		return 0, nil
	}

	u64, err = strconv.ParseUint(string(val), 10, 0)
	return
}

// FirstIndex, LastIndex, GetLog, StoreLog, StoreLogs, DeleteRange は、raft.LogStore を実装
func (r *RocksDBStore) FirstIndex() (index uint64, err error) {
	defer func() {
		r.logger.Debug("FirstIndex", "firstIndex", fmt.Sprint(index))
	}()

	it := r.db.NewIterator(defaultReadOptions)
	defer it.Close()
	if it.SeekToFirst(); it.Valid() {
		key := it.Key()
		defer key.Free()
		u64, err := strconv.ParseUint(string(key.Data()), 10, 0)
		if err != nil {
			return 0, err
		}
		index = u64
	}
	return
}

func (r *RocksDBStore) LastIndex() (index uint64, err error) {
	defer func() {
		r.logger.Debug("LastIndex", "lastIndex", fmt.Sprint(index))
	}()

	it := r.db.NewIterator(defaultReadOptions)
	defer it.Close()
	if it.SeekToLast(); it.Valid() {
		key := it.Key()
		defer key.Free()
		u64, err := strconv.ParseUint(string(key.Data()), 10, 0)
		if err != nil {
			return 0, err
		}
		index = u64
	}
	return
}

func (r *RocksDBStore) GetLog(index uint64, log *raft.Log) (err error) {
	var val []byte
	defer func() {
		r.logger.Debug("GetLog", "index", fmt.Sprint(index), "log", string(val))
	}()

	val, err = r.Get([]byte(strconv.FormatUint(index, 10)))
	if err != nil {
		return err
	}
	if val == nil {
		return raft.ErrLogNotFound
	}

	return json.Unmarshal(val, log)
}

func (r *RocksDBStore) StoreLog(log *raft.Log) (err error) {
	var val []byte
	defer func() {
		r.logger.Debug("StoreLog", "log", string(val))
	}()

	val, err = json.Marshal(log)
	if err != nil {
		return err
	}

	return r.Set([]byte(strconv.FormatUint(log.Index, 10)), val)
}

func (r *RocksDBStore) StoreLogs(logs []*raft.Log) (err error) {
	var debugLogs []interface{}
	for i, log := range logs {
		val, err := json.Marshal(log)
		if err != nil {
			return err
		}
		debugLogs = append(debugLogs, fmt.Sprintf("logs[%d]", i))
		debugLogs = append(debugLogs, string(val))
	}
	r.logger.Debug("StoreLog", debugLogs...)

	wb := gorocksdb.NewWriteBatch()
	for _, log := range logs {
		val, err := json.Marshal(log)
		if err != nil {
			return err
		}

		wb.Put([]byte(strconv.FormatUint(log.Index, 10)), val)
	}

	return r.db.Write(gorocksdb.NewDefaultWriteOptions(), wb)
}

func (r *RocksDBStore) DeleteRange(min uint64, max uint64) error {
	r.logger.Debug("DeleteRange", "min", min, "max", max)
	wb := gorocksdb.NewWriteBatch()

	it := r.db.NewIterator(gorocksdb.NewDefaultReadOptions())

	start := []byte(strconv.FormatUint(min, 10))
	for it.Seek(start); it.Valid(); it.Next() {
		key := it.Key()
		defer key.Free()
		u64Key, err := strconv.ParseUint(string(key.Data()), 10, 0)
		if err != nil {
			return err
		}

		if u64Key > max {
			break
		}

		wb.Delete(key.Data())
	}
	it.Close()

	return r.db.Write(gorocksdb.NewDefaultWriteOptions(), wb)
}
