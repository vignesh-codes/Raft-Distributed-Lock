package raft

import (
	"sync"
)

type Storage interface {
	HasData() bool
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
}

type Database struct {
	mu sync.Mutex
	kv map[string][]byte
}

func NewDatabase() *Database {
	return &Database{
		kv: make(map[string][]byte),
	}
}

func (db *Database) Get(key string) ([]byte, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	value, found := db.kv[key]
	return value, found
}

func (db *Database) Set(key string, value []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.kv[key] = value
}

func (db *Database) HasData() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.kv) > 0
}

func (db *Database) Delete(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.kv, key)
}

func (db *Database) Exists(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, ok := db.kv[key]
	return ok
}

func (db *Database) Keys() []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	keys := make([]string, 0, len(db.kv))
	for k := range db.kv {
		keys = append(keys, k)
	}
	// fmt.Printf("KEYS: %v\n", keys)
	return keys
}
