package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	nullBulkString = "$-1\r\n"
)

type handler func(args []string) string

type item struct {
	value  any
	expiry time.Time
}

type Cache struct {
	items    map[string]item
	mu       sync.Mutex
	interval time.Duration
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	cache := &Cache{
		items:    make(map[string]item),
		interval: time.Minute,
	}

	go cache.reapLoop()

	var commands = map[string]handler{
		"PING":  cmdPing,
		"ECHO":  cmdEcho,
		"SET":   cache.cmdSet,
		"GET":   cache.cmdGet,
		"RPUSH": cache.cmdRpush,
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

	item := item{
		value:  args[1],
		expiry: time.Time{}, //zero value
	}

	if len(args) > 3 {
		duration, err := strconv.Atoi(args[3])
		if err != nil {
			return "-ERR error parsing expiration time\r\n"
		}
		switch args[2] {
		case "EX":
			item.expiry = time.Now().Add(time.Duration(duration))
		case "PX":
			item.expiry = time.Now().Add(time.Duration(duration) * time.Millisecond)
		default:
			return "-ERR unsupported expiration option\r\n"
		}
	}

	key := args[0]

	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = item

	return "+OK\r\n"
}

func (c *Cache) cmdGet(args []string) string {
	if len(args) == 0 {
		return "-ERR wrong number of arguments\r\n"
	}

	// log.Printf("%#v\n", args)

	key := args[0]

	c.mu.Lock()
	defer c.mu.Unlock()

	if i, ok := c.items[key]; ok && !i.isExpired() {
		switch v := i.value.(type) {
		case string:
			return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)

		case []string:
			// return fmt.Sprintf("*/d\r\n")

		default:
			fmt.Println("unknown")
		}
	}

	return nullBulkString
}

func (c *Cache) cmdRpush(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments\r\n"
	}

	key := args[0]
	values := args[1:]
	length := len(args[1:])

	c.mu.Lock()
	defer c.mu.Unlock()

	i, exists := c.items[key]
	if exists {
		if list, ok := i.value.([]string); ok {
			list = append(list, values...)
			i.value = list
			length = len(list)
		} else {
			return "-ERR wrong type\r\n"
		}
	} else {
		i = item{
			value:  values,
			expiry: time.Time{},
		}
	}

	c.items[key] = i

	return fmt.Sprintf(":%d\r\n", length)
}

// --- Cache clear ---

func (c *Cache) reapLoop() {
	ticker := time.NewTicker(c.interval)
	for range ticker.C {
		c.mu.Lock()
		defer c.mu.Unlock()
		for key, item := range c.items {
			if item.isExpired() {
				delete(c.items, key)
			}
		}
	}
}

func (i item) isExpired() bool {
	if i.expiry.IsZero() {
		return false
	}
	return time.Now().After(i.expiry)
}
