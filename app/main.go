package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {

	defer conn.Close()

	reader := bufio.NewReader(conn)
	// for {
	_, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Read error: %v", err)
		return
	}

	// ackMsg := strings.ToUpper(strings.TrimSpace(message))
	response := fmt.Sprint("+PONG\r\n")
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Printf("Server write error: %v", err)
	}
	// }
}
