package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

type handler func(args []string) string

type Cache struct {
	items map[string]string
	mu    sync.Mutex
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	cache := &Cache{
		items: make(map[string]string),
	}

	var commands = map[string]handler{
		"PING": cmdPing,
		"ECHO": cmdEcho,
		"SET":  cache.cmdSet,
		"GET":  cache.cmdGet,
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			os.Exit(1)
		}
		go handleConnection(conn, commands)
	}
}

// --- RESP parser ---

func readCommand(r *bufio.Reader) ([]string, error) {
	// Expect *<count>\r\n
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	// log.Printf("%q\n", line)
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
		// log.Printf("%q\n", val)
		args[i] = strings.TrimSpace(val)
	}
	return args, nil
}

// --- Command dispatcher ---

func handleConnection(conn net.Conn, commands map[string]handler) {
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

	s := strings.Join(args, " ")
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func (c *Cache) cmdSet(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments\r\n"
	}

	key := args[0]
	value := args[1]

	c.items[key] = value

	return "+OK\r\n"
}

func (c *Cache) cmdGet(args []string) string {
	if len(args) == 0 {
		return "-ERR wrong number of arguments\r\n"
	}

	// log.Printf("%#v\n", args)

	s := c.items[args[0]]

	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
