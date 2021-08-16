package raftdb

import (
	"encoding/json"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"

	"github.com/flier/gorocksdb"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

func newDBAndStore(t *testing.T) (*gorocksdb.DB, *RocksDBStore, func()) {
	dir := path.Join(os.TempDir(), "raftdb_store_test", uuid.NewString())
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, dir)
	store := &RocksDBStore{db, zap.NewExample()}
	if err != nil {
		t.Fatal(err)
	}

	return db, store, db.Close
}

func TestRocksDBSet(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name       string
		expected   error
		givenKey   string
		givenValue string
	}{
		{"キーと値を保存", nil, "samplekey", "samplevalue"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			actual := store.Set([]byte(tt.givenKey), []byte(tt.givenValue))
			if actual != tt.expected {
				t.Errorf("(%s): expected %s, actual %s", tt.givenKey, tt.expected, actual)
			}

			value, err := db.Get(defaultReadOptions, []byte(tt.givenKey))
			if err != nil {
				t.Fatal(err)
			}
			defer value.Free()
			if string(value.Data()) != tt.givenValue {
				t.Errorf("(%s): expected %s, actual %s", tt.givenKey, tt.givenValue, value.Data())
			}
		})
	}
}

func TestRocksDBGet(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		prefunc       func()
		expectedError error
		expectedValue []byte
		given         string
	}{
		{"キーが存在する場合は値を取得", func() {
			err := db.Put(defaultWriteOptions, []byte("samplekey"), []byte("samplevalue"))
			if err != nil {
				t.Fatal(err)
			}
		}, nil, []byte("samplevalue"), "samplekey"},
		{"キーが存在しない場合は値はnilが帰る", func() {}, nil, nil, "not_samplekey"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.prefunc()

			actual, err := store.Get([]byte(tt.given))
			if err != tt.expectedError {
				t.Errorf("(%s): expected %s, actual %s", tt.given, tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("(%s): expected %s, actual %s", tt.given, tt.expectedValue, actual)
			}
		})
	}
}

func TestRocksDBSetUint64(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name       string
		expected   error
		givenKey   string
		givenValue uint64
	}{
		{"キーと値を保存", nil, "samplekey", 123},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			actual := store.SetUint64([]byte(tt.givenKey), tt.givenValue)
			if actual != tt.expected {
				t.Errorf("(%s): expected %s, actual %s", tt.givenKey, tt.expected, actual)
			}

			value, err := db.Get(defaultReadOptions, []byte(tt.givenKey))
			if err != nil {
				t.Fatal(err)
			}
			defer value.Free()
			if !reflect.DeepEqual(value.Data(), []byte(strconv.FormatUint(tt.givenValue, 10))) {
				t.Errorf("(%s): expected %d, actual %d", []byte(strconv.FormatUint(tt.givenValue, 10)), tt.givenValue, value.Data())
			}
		})
	}
}

func TestRocksDBGetUint64(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		prefunc       func()
		expectedError error
		expectedValue uint64
		given         string
	}{
		{"キーが存在する場合は値を取得", func() {
			err := db.Put(defaultWriteOptions, []byte("samplekey"), []byte("1"))
			if err != nil {
				t.Fatal(err)
			}
		}, nil, 1, "samplekey"},
		{"キーが存在しない場合は値は0が帰る", func() {}, nil, 0, "not_samplekey"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.prefunc()

			actual, err := store.GetUint64([]byte(tt.given))
			if err != tt.expectedError {
				t.Errorf("(%s): expected %s, actual %s", tt.given, tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("(%s): expected %d, actual %d", tt.given, tt.expectedValue, actual)
			}
		})
	}
}

func TestFirstIndex(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		prefunc       func()
		expectedError error
		expectedValue uint64
	}{
		{"最初のインデックスを取得", func() {
			err := db.Put(defaultWriteOptions, []byte("1"), []byte("data1"))
			if err != nil {
				t.Fatal(err)
			}
			err = db.Put(defaultWriteOptions, []byte("2"), []byte("data2"))
			if err != nil {
				t.Fatal(err)
			}
		}, nil, 1},
		{"最初のインデックスを取得", func() {
			err := db.Put(defaultWriteOptions, []byte("3"), []byte("data3"))
			if err != nil {
				t.Fatal(err)
			}
		}, nil, 1},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.prefunc()

			actual, err := store.FirstIndex()
			if err != tt.expectedError {
				t.Errorf("expected %s, actual %s", tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("expected %d, actual %d", tt.expectedValue, actual)
			}
		})
	}
}

func TestLastIndex(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		prefunc       func()
		expectedError error
		expectedValue uint64
	}{
		{"最後のインデックスを取得", func() {
			err := db.Put(defaultWriteOptions, []byte("1"), []byte("data1"))
			if err != nil {
				t.Fatal(err)
			}
			err = db.Put(defaultWriteOptions, []byte("2"), []byte("data2"))
			if err != nil {
				t.Fatal(err)
			}
		}, nil, 2},
		{"最後のインデックスを取得", func() {
			err := db.Put(defaultWriteOptions, []byte("3"), []byte("data3"))
			if err != nil {
				t.Fatal(err)
			}
		}, nil, 3},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.prefunc()

			actual, err := store.LastIndex()
			if err != tt.expectedError {
				t.Errorf("expected %v, actual %v", tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("expected %v, actual %v", tt.expectedValue, actual)
			}
		})
	}
}

func TestGetLog(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name          string
		prefunc       func()
		expectedError error
		expectedValue *raft.Log
		given         uint64
	}{
		{"ログを取得", func() {
			log := &raft.Log{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value")}
			jsonLog, err := json.Marshal(log)
			if err != nil {
				t.Fatal(err)
			}

			if err := db.Put(defaultWriteOptions, []byte("123"), jsonLog); err != nil {
				t.Fatal(err)
			}
		}, nil, &raft.Log{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value")}, 123},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.prefunc()

			actual := new(raft.Log)
			err := store.GetLog(tt.given, actual)
			if err != tt.expectedError {
				t.Errorf("expected %v, actual %v", tt.expectedError, err)
			}

			if !reflect.DeepEqual(actual, tt.expectedValue) {
				t.Errorf("expected %v, actual %v", tt.expectedValue, actual)
			}
		})
	}
}

func TestStoreLog(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name     string
		expected error
		given    *raft.Log
	}{
		{"ログを保存", nil, &raft.Log{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value")}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := store.StoreLog(tt.given)
			if err != tt.expected {
				t.Errorf("expected %v, actual %v", tt.expected, err)
			}

			value, err := db.Get(defaultReadOptions, []byte(strconv.FormatUint(tt.given.Index, 10)))
			if err != nil {
				t.Fatal(err)
			}
			defer value.Free()

			actual := new(raft.Log)
			if err := json.Unmarshal(value.Data(), actual); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(actual, tt.given) {
				t.Errorf("expected %v, actual %v", tt.given, actual)
			}
		})
	}
}

func TestStoreLogs(t *testing.T) {
	db, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name     string
		expected error
		given    []*raft.Log
	}{
		{"ログを保存", nil, []*raft.Log{
			{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value")},
			{Type: raft.LogCommand, Index: 124, Term: 457, Data: []byte("value2")},
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := store.StoreLogs(tt.given)
			if err != tt.expected {
				t.Errorf("expected %v, actual %v", tt.expected, err)
			}

			for _, given := range tt.given {
				value, err := db.Get(defaultReadOptions, []byte(strconv.FormatUint(given.Index, 10)))
				if err != nil {
					t.Fatal(err)
				}
				defer value.Free()

				actual := new(raft.Log)
				if err := json.Unmarshal(value.Data(), actual); err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(actual, given) {
					t.Errorf("expected %v, actual %v", given, actual)
				}
			}
		})
	}
}

func TestDeleteRange(t *testing.T) {
	_, store, finish := newDBAndStore(t)
	defer finish()

	var tests = []struct {
		name     string
		prefunc  func()
		postfunc func()
		expected error
		givenMin uint64
		givenMax uint64
	}{
		{"ログを削除", func() {
			logs := []*raft.Log{
				{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value1")},
				{Type: raft.LogCommand, Index: 124, Term: 457, Data: []byte("value2")},
				{Type: raft.LogCommand, Index: 125, Term: 458, Data: []byte("value3")},
				{Type: raft.LogCommand, Index: 126, Term: 459, Data: []byte("value4")},
				{Type: raft.LogCommand, Index: 127, Term: 460, Data: []byte("value5")},
			}
			if err := store.StoreLogs(logs); err != nil {
				t.Fatal(err)
			}
		},
			func() {
				logs := []*raft.Log{
					{Type: raft.LogCommand, Index: 123, Term: 456, Data: []byte("value1")},
					{Type: raft.LogCommand, Index: 127, Term: 460, Data: []byte("value5")},
				}
				for _, log := range logs {
					l := new(raft.Log)
					if err := store.GetLog(log.Index, l); err != nil {
						t.Fatal(err)
					}

					if !reflect.DeepEqual(log, l) {
						t.Errorf("expected %v, actual %v", log, l)
					}
				}

				logs = []*raft.Log{
					{Type: raft.LogCommand, Index: 124, Term: 457, Data: []byte("value2")},
					{Type: raft.LogCommand, Index: 125, Term: 458, Data: []byte("value3")},
					{Type: raft.LogCommand, Index: 126, Term: 459, Data: []byte("value4")},
				}
				for _, log := range logs {
					l := new(raft.Log)
					err := store.GetLog(log.Index, l)
					if err != raft.ErrLogNotFound {
						t.Errorf("expected %v, actual %v", raft.ErrLogNotFound, err)
					}
				}
			},
			nil, 124, 126},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.prefunc()
			err := store.DeleteRange(tt.givenMin, tt.givenMax)
			if err != tt.expected {
				t.Errorf("expected %v, actual %v", tt.expected, err)
			}
			tt.postfunc()
		})
	}
}
