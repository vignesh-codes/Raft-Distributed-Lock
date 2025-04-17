package client

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type DataEntry struct {
	ClientID          string    `json:"clientID"`
	Timestamp         time.Time `json:"timestamp"`
	FencingTokenKey   string    `json:"fencingTokenKey"`
	FencingTokenValue uint64    `json:"fencingTokenValue"`
	Data              string    `json:"data"`
}

type WriteRequest struct {
	ClientID     string       `json:"clientID"`
	FencingToken FencingToken `json:"fencingToken"`
	Data         string       `json:"data"`
}

type WriteResponse struct {
	Success bool `json:"success"`
}

var (
	maxFencingTokenSoFar map[string]FencingToken
	tokenMutex           sync.Mutex
	dataFilePath         = "client/data.txt"
	dataStoreURL         = "http://localhost:8000/write"
)

func writeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed; use POST", http.StatusMethodNotAllowed)
		return
	}

	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	tokenMutex.Lock()
	defer tokenMutex.Unlock()

	if maxToken, exists := maxFencingTokenSoFar[req.FencingToken.Key]; exists && req.FencingToken.Value < maxToken.Value {
		resp := WriteResponse{
			Success: false,
		}
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(resp)
		return
	}

	maxFencingTokenSoFar[req.FencingToken.Key] = req.FencingToken

	entry := DataEntry{
		ClientID:          req.ClientID,
		Timestamp:         time.Now(),
		FencingTokenKey:   req.FencingToken.Key,
		FencingTokenValue: req.FencingToken.Value,
		Data:              req.Data,
	}

	f, err := os.OpenFile(dataFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		http.Error(w, "Failed to open log file", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	entryJSON, err := json.Marshal(entry)
	if err != nil {
		http.Error(w, "Error processing log entry", http.StatusInternalServerError)
		return
	}

	if _, err = f.Write(append(entryJSON, '\n')); err != nil {
		http.Error(w, "Error writing log entry", http.StatusInternalServerError)
		return
	}

	resp := WriteResponse{
		Success: true,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func Data_store_init() {
	maxFencingTokenSoFar = make(map[string]FencingToken)
	http.HandleFunc("/write", writeHandler)
	log.Printf("Server is running on port %d...\n", 8000)
	err := http.ListenAndServe(":"+strconv.Itoa(8000), nil)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
