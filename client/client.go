package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type LockCommandType int

const (
	LockAcquire LockCommandType = iota
	LockRelease
)

type FencingToken struct {
	Key   string
	Value uint64
}

type LockRequest struct {
	CommandType LockCommandType `json:"commandType"`
	Key         string          `json:"key"`
	ClientID    string          `json:"clientId"`
	TTL         time.Duration   `json:"ttl"`
}
type LockAcquireReply struct {
	Success      bool
	FencingToken FencingToken
}

type ConnectionRequest struct {
	ClientID string `json:"clientID"`
}

type ConnectionReply struct {
	Success bool
	Leader  int64
}

var (
	Conn          *websocket.Conn
	ClientID      string
	servers       []uint64
	responseChan  chan []byte
	reconnectedCh chan struct{}
	fencingToken  map[string]FencingToken
)

func selectRandomServer(serverId int64) uint64 {
	for {
		idx := rand.Intn(len(servers))
		if serverId != int64(servers[idx]) {
			return servers[idx]
		}
	}
}

func connectToLeader() {
	serverId := selectRandomServer(-1)
	for {
		// log.Printf("Attempting to connect to candidate server %d...", serverId)
		err := connectToServer(serverId)
		if err != nil {
			// log.Printf("Error connecting to server %d: %v", serverId, err)
			serverId = selectRandomServer(int64(serverId))
			continue
		}
		_, msg, err := Conn.ReadMessage()
		if err != nil {
			// log.Printf("Error reading from server %d: %v", serverId, err)
			Conn.Close()
			serverId = selectRandomServer(int64(serverId))
			continue
		}

		var reply ConnectionReply
		err = json.Unmarshal(msg, &reply)
		if err != nil {
			// log.Printf("Error decoding connection reply from server %d: %v", serverId, err)
			Conn.Close()
			serverId = selectRandomServer(int64(serverId))
			continue
		}

		if !reply.Success {
			// log.Printf("Non Leader Server %d indicated leader is %d", serverId, reply.Leader)
			Conn.Close()
			if reply.Leader == -1 {
				serverId = selectRandomServer(int64(serverId))
			} else {
				serverId = uint64(reply.Leader)
			}
			continue
		}

		if reply.Success {
			log.Printf("Connected to leader %d", serverId)
			return
		}
	}
}

func startReader() {
	for {
		responseChan = make(chan []byte)
		// fmt.Printf("finding connection...\n")
		connectToLeader()
		// fmt.Printf("reestablished connection\n")
		select {
		case reconnectedCh <- struct{}{}:
		default:
			<-reconnectedCh
		}

		for {
			// fmt.Printf("Ready to Listen...\n")
			_, message, err := Conn.ReadMessage()
			if err != nil {
				log.Printf("Connection lost: %v", err)
				close(responseChan)
				Conn.Close()
				break
			}
			// fmt.Printf("Message received\n")

			responseChan <- message
		}
	}
}

func connectToServer(serverId uint64) error {
	port := fmt.Sprintf("%d", 50050+serverId)
	url := fmt.Sprintf("ws://localhost:%s/ws", port)
	var err error
	Conn, _, err = websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	connectionReq := ConnectionRequest{
		ClientID: ClientID,
	}

	data, err := json.Marshal(connectionReq)
	if err != nil {
		// fmt.Printf("json marshal error: %v\n", err)
		return err
	}

	// log.Printf("Connecting to server %d at %v\n", serverId, url)

	err = Conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		return err
	}
	// log.Printf("Sent client ID: %s", ClientID)

	return nil
}

func acquireLock(key string, ttl int64) {
	for {
		lockReq := LockRequest{
			CommandType: LockAcquire,
			Key:         key,
			ClientID:    ClientID,
			TTL:         time.Duration(ttl) * time.Second,
		}

		data, err := json.Marshal(lockReq)
		if err != nil {
			log.Printf("json marshal error: %v", err)
			return
		}

		// Retry loop for sending requests
		for {
			if Conn == nil {
				// log.Println("Waiting for reconnection...")
				<-reconnectedCh
				continue
			}
			err := Conn.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				log.Printf("error sending lock command: %v", err)
				time.Sleep(time.Second)
				continue
			}
			log.Printf("Sent lock acquire command: %s", data)
			break
		}

		select {
		case message, ok := <-responseChan:
			if !ok {
				log.Println("Connection lost, waiting for reconnection...")
				<-reconnectedCh
				continue
			}
			// fmt.Printf("Message: %v\n", message)
			var reply LockAcquireReply
			err = json.Unmarshal(message, &reply)
			if err != nil {
				log.Printf("Invalid response: %v", err)
				continue
			}

			if reply.Success {
				log.Printf("Lock %s acquired successfully with token %v\n", key, reply.FencingToken)
				fencingToken[reply.FencingToken.Key] = reply.FencingToken
				return
			} else {
				log.Printf("Lock %s acquisition failed, retrying...", key)
				time.Sleep(2 * time.Second)
			}

		case <-time.After(1000 * time.Second):
			log.Println("Timed out waiting for response, retrying...")
		}
	}
}

func releaseLock(key string) {
	if Conn == nil {
		fmt.Printf("locking service connection missing\n")
		return
	}
	lockReq := LockRequest{
		CommandType: LockRelease,
		Key:         key,
		ClientID:    ClientID,
		TTL:         time.Duration(0),
	}
	data, err := json.Marshal(lockReq)
	if err != nil {
		fmt.Printf("json marshal error: %v\n", err)
		return
	}

	err = Conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		fmt.Printf("error sending lock command: %v\n", err)
		return
	}
	log.Printf("Sent lock release command: %s", data)
}

func writeData(data string, lockKey string) {
	reqPayload := WriteRequest{
		ClientID:     ClientID,
		FencingToken: fencingToken[lockKey],
		Data:         data,
	}

	jsonData, err := json.Marshal(reqPayload)
	if err != nil {
		log.Fatalf("Error marshalling JSON: %v", err)
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Post(dataStoreURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("Error making POST request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response: %v", err)
	}

	var writeResp WriteResponse
	if err = json.Unmarshal(body, &writeResp); err != nil {
		log.Fatalf("Error decoding response JSON: %v", err)
	}

	if writeResp.Success {
		fmt.Println("Write operation was successful!")
	} else {
		fmt.Printf("Write operation failed. HTTP status: %d\n", resp.StatusCode)
	}
}

func PrintMenu() {
	fmt.Println("\n\n           	                 CLIENT MENU:")
	fmt.Println("+--------------------------------------+------------------------------------+")
	fmt.Println("| Sr |  USER COMMANDS                  |      ARGUMENTS                     |")
	fmt.Println("+----+---------------------------------+------------------------------------+")
	fmt.Println("| 1  | create client                   |      clientId                      |")
	fmt.Println("| 2  | connect to locking service      |      serverId upperlimit           |")
	fmt.Println("| 3  | acquire lock                    |      lockKey, TTL (in secs)        |")
	fmt.Println("| 4  | release lock                    |      lockKey                       |")
	fmt.Println("| 5  | write data                      |      message, lockKey              |")
	fmt.Println("+----+---------------------------------+------------------------------------+")
	fmt.Println("+---------------------------------------------------------------------------+")
	fmt.Println("")
}

func ClientInput(sigCh chan os.Signal) {
	go func() {
		<-sigCh
		fmt.Println("SIGNAL RECEIVED")
		os.Exit(0)
	}()

	reconnectedCh = make(chan struct{}, 1)
	fencingToken = make(map[string]FencingToken)
	for {
		PrintMenu()
		fmt.Println("WAITING FOR INPUTS..")
		fmt.Println("")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		// input = strings.TrimSpace(input)
		tokens := strings.Fields(input)
		command, err0 := strconv.Atoi(tokens[0])
		if err0 != nil {
			fmt.Println("Wrong input")
			continue
		}
		switch command {
		case 1:
			if len(tokens) < 2 {
				fmt.Println("ClientID not passed")
				break
			}
			ClientID = tokens[1]
			break
		case 2:
			if len(tokens) < 2 {
				fmt.Println("Locking ServerId range not passed")
				break
			}
			serverIdRange, err := strconv.Atoi(tokens[1])
			if err != nil {
				fmt.Println("invalid number of peers")
				break
			}
			for i := 0; i < serverIdRange; i++ {
				servers = append(servers, uint64(i))
			}
			go startReader()
		case 3:
			if len(tokens) < 3 {
				fmt.Printf("Lock Key and TTL not passed")
				break
			}
			ttl, err := strconv.Atoi(tokens[2])
			if err != nil {
				fmt.Println("invalid TTL")
				break
			}
			go acquireLock(tokens[1], int64(ttl))
		case 4:
			if len(tokens) < 2 {
				fmt.Printf("Lock Key not passed")
				break
			}
			go releaseLock(tokens[1])
		case 5:
			if len(tokens) < 3 {
				fmt.Printf("Message not passed\n")
				break
			}
			writeData(tokens[1], tokens[2])
		default:
			fmt.Printf("Invalid input")
		}
	}
}
