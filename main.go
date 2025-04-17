package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/FitrahHaque/raft-consensus/client"
	"github.com/FitrahHaque/raft-consensus/raft"
)

func main() {
	var input string

	// var server *raft.Server = nil
	// var peerId int = 0

	// sigCh := make(chan os.Signal)
	// signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// go func() {
	// 	<-sigCh
	// 	fmt.Println("SIGNAL RECEIVED")
	// 	Stop(server)
	// 	os.Exit(0)
	// }()
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	fmt.Println("\n\n=============================================================")
	fmt.Println("...........Choose CLIENT or SERVER.............")
	fmt.Println("=============================================================")

	for {
		fmt.Println("WAITING FOR INPUTS..")
		fmt.Println("Press\nS for Server\nC for Client\nD for Data Store\n")

		reader := bufio.NewReader(os.Stdin)
		input, _ = reader.ReadString('\n')
		tokens := strings.Fields(input)
		switch tokens[0] {
		case "C":
			client.ClientInput(sigCh)
		case "S":
			raft.ServerInput(sigCh)
		case "D":
			go client.Data_store_init()
		default:
			fmt.Println("Invalid Command")
		}
		fmt.Println("\n---------------------------------------------------------")
	}
}
