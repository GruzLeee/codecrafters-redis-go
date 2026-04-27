package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

// --- RESP parser ---

func readCommand(r *bufio.Reader) ([]string, error) {
	// Expect *<count>\r\n
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, count)
	for i := range args {
		// $<len>\r\n
		if _, err := r.ReadString('\n'); err != nil {
			return nil, err
		}
		// <value>\r\n
		val, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		args[i] = strings.TrimSpace(val)
	}
	return args, nil
}

// --- Command dispatcher ---

type handler func(args []string) string

var commands = map[string]handler{
	"PING": cmdPing,
	"ECHO": cmdEcho,
	// "SET": cmdSet,
	// "GET": cmdGet,
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		name := strings.ToUpper(args[0])
		h, ok := commands[name]
		var resp string
		if ok {
			resp = h(args[1:])
		} else {
			resp = "-ERR unknown command '" + name + "'\r\n"
		}
		conn.Write([]byte(resp))
	}
}

// --- Handlers ---

func cmdPing(args []string) string {
	if len(args) > 0 {
		return "+" + args[0] + "\r\n"
	}
	return "+PONG\r\n"
}

func cmdEcho(args []string) string {
	if len(args) == 0 {
		return "-ERR wrong number of arguments\r\n"
	}

	length := len(args[0])

	return fmt.Sprintf("$%d\r\n%s\r\n", length, args[0])
}
