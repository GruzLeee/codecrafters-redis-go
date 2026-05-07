package main

import (
	"bufio"
	"fmt"
	"log"
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

type restArrayElement struct {
	next *restArrayElement
	data string
}

type restArray struct {
	head  *restArrayElement
	tail  *restArrayElement
	count int
}

type Cache struct {
	items    map[string]*item
	mu       sync.Mutex
	interval time.Duration
	popQueue map[string]*consumerQueue
}

type consumerQueue struct {
	head  *consumer
	tail  *consumer
	count int
}

type consumer struct {
	next *consumer
	prev *consumer
	ch   chan *restArray
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	cache := &Cache{
		items:    make(map[string]*item),
		interval: time.Hour,
		popQueue: make(map[string]*consumerQueue),
	}

	go cache.reapLoop()

	var commands = map[string]handler{
		"PING":   cmdPing,
		"ECHO":   cmdEcho,
		"SET":    cache.cmdSet,
		"GET":    cache.cmdGet,
		"RPUSH":  cache.cmdRpush,
		"LRANGE": cache.cmdLrange,
		"LPUSH":  cache.cmdLpush,
		"LLEN":   cache.cmdLlen,
		"LPOP":   cache.cmdLpop,
		"BLPOP":  cache.cmdBlpop,
		"TYPE":   cache.cmdType,
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
		// log.Print(conn.RemoteAddr())
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

	c.items[key] = &item

	return "+OK\r\n"
}

func (c *Cache) cmdGet(args []string) string {
	if len(args) == 0 {
		return "-ERR wrong number of arguments\r\n"
	}

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

func (c *Cache) cmdLrange(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments\r\n"
	}

	key := args[0]

	start, err := strconv.Atoi(args[1])
	if err != nil {
		return "-ERR start must be a valid integer\r\n"
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return "-ERR stop must be a valid integer\r\n"
	}

	if item, exists := c.items[key]; exists {
		if list, ok := item.value.(*restArray); ok {
			listLen := list.count
			if start < 0 {
				if start < -listLen {
					start = 0
				} else {
					start += listLen
				}
			}
			if stop < 0 {
				if stop < -listLen {
					stop = 0
				} else {
					stop += listLen
				}
			}
			if start > listLen || start > stop {
				return "*0\r\n"
			}
			if stop > listLen {
				stop = listLen - 1
			}
			var respArray strings.Builder
			fmt.Fprintf(&respArray, "*%d\r\n", stop-start+1)
			i := 0
			for curr := list.head; curr != nil; curr = curr.next {
				if i >= start && i <= stop {
					fmt.Fprintf(&respArray, "$%d\r\n%s\r\n", len(curr.data), curr.data)
				}
				i++
			}
			return respArray.String()
		}
		return "-ERR wrong type\r\n"
	}
	return "*0\r\n"
}

func (c *Cache) cmdLpush(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments\r\n"
	}

	key := args[0]
	values := args[1:]
	length := len(values)

	head := restArrayElement{
		data: values[length-1],
	}
	tail := &head
	for i := length - 2; i >= 0; i-- {
		tail.next = &restArrayElement{
			data: values[i],
		}
		tail = tail.next
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if i, exists := c.items[key]; exists {
		if list, ok := i.value.(*restArray); ok {
			tail.next = list.head
			list.head = &head
			list.count += length
			length = list.count
		} else {
			return "-ERR \r\n"
		}
	} else {
		i = &item{
			value: &restArray{
				head:  &head,
				tail:  tail,
				count: length,
			},
			expiry: time.Time{},
		}
		c.items[key] = i
	}
	if queue, exists := c.popQueue[key]; exists {
		consumer := queue.head
		for i := length; i > 0 && consumer != nil; i-- {
			consumer.ch <- c.items[key].value.(*restArray)
			consumer = consumer.next
		}
	}

	return fmt.Sprintf(":%d\r\n", length)
}

func (c *Cache) cmdLlen(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments\r\n"
	}

	key := args[0]

	if i, exists := c.items[key]; exists {
		if list, ok := i.value.(*restArray); ok {
			return fmt.Sprintf(":%d\r\n", list.count)
		}
	}
	return ":0\r\n"
}

func (c *Cache) cmdLpop(args []string) string {
	if len(args) < 1 {
		return "-ERR wrong number of parameters\r\n"
	}

	key := args[0]
	count := 1
	var err error
	if len(args) > 1 {
		count, err = strconv.Atoi(args[1])
		if err != nil {
			return "-ERR invalid range\r\n"
		}
	}

	fmt.Println(count)
	if i, exists := c.items[key]; exists {
		if list, ok := i.value.(*restArray); ok {
			if list.count > 0 {
				if count > list.count {
					count = list.count
				}
				var respArray strings.Builder
				for i := count; i > 0; i-- {
					pop := list.head
					list.head = list.head.next
					list.count--
					fmt.Fprintf(&respArray, "$%d\r\n%s\r\n", len(pop.data), pop.data)
				}
				if list.count == 0 {
					delete(c.items, key)
				}
				if count > 1 {
					return fmt.Sprintf("*%d\r\n%s", count, &respArray)
				} else {
					return respArray.String()
				}
			}
		}
	}

	return "$-1\r\n"

}

func (c *Cache) cmdRpush(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments\r\n"
	}

	key := args[0]
	values := args[1:]
	length := len(args[1:])

	head := restArrayElement{
		data: values[0],
	}
	tail := &head
	for _, v := range values[1:] {
		tail.next = &restArrayElement{
			data: v,
		}
		tail = tail.next
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if i, exists := c.items[key]; exists {
		if list, ok := i.value.(*restArray); ok {
			list.tail.next = &head
			list.tail = tail
			list.count += length
			length = list.count
		} else {
			return "-ERR\r\n"
		}
	} else {
		i = &item{
			value: &restArray{
				head:  &head,
				tail:  tail,
				count: length,
			},
			expiry: time.Time{},
		}
		c.items[key] = i
	}

	if queue, exists := c.popQueue[key]; exists {
		consumer := queue.head
		for i := length; i > 0 && consumer != nil; i-- {
			consumer.ch <- c.items[key].value.(*restArray)
			consumer = consumer.next
		}
	}
	return fmt.Sprintf(":%d\r\n", length)
}

func (c *Cache) cmdBlpop(args []string) string {
	if len(args) != 2 {
		return "-ERR wrong number of parameters\r\n"
	}

	key := args[0]
	expiration, err := time.ParseDuration(args[1] + "s")
	if err != nil {
		return "-ERR invalid expiration time \r\n"
	}
	if expiration == 0 {
		expiration = time.Hour * 0x7fff
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if i, exists := c.items[key]; exists {
		if list, ok := i.value.(*restArray); ok {
			if list.count > 0 {
				pop := list.head
				list.head = list.head.next
				list.count--
				if list.count == 0 {
					delete(c.items, key)
				}
				return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(pop.data), pop.data)
			}
		} else {
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
		}
	}

	newConsumer := &consumer{
		ch: make(chan *restArray, 1),
	}
	queue, exists := c.popQueue[key]
	if exists {
		newConsumer.prev = queue.tail
		queue.tail.next = newConsumer
		queue.tail = newConsumer
		queue.count++
	} else {
		newConsumerQueue := &consumerQueue{
			head:  newConsumer,
			tail:  newConsumer,
			count: 1,
		}
		c.popQueue[key] = newConsumerQueue
	}
	c.mu.Unlock()

	var retString string

	select {
	case <-time.After(expiration):
		c.mu.Lock()
		retString = "*-1\r\n"

	case restArray := <-newConsumer.ch:
		c.mu.Lock()
		pop := restArray.head
		restArray.head = restArray.head.next
		restArray.count--
		if restArray.count == 0 {
			delete(c.items, key)
		}
		retString = fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(pop.data), pop.data)
	}

	c.popQueue[key].count--
	if c.popQueue[key].count == 0 {
		delete(c.popQueue, key)
	} else if c.popQueue[key].head == newConsumer {
		c.popQueue[key].head = c.popQueue[key].head.next
	} else {
		newConsumer.prev.next = newConsumer.next
		newConsumer.next = newConsumer.prev
	}

	return retString
}

func (c *Cache) cmdType(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'type' command"
	}

	key := args[0]
	retType := "none"
	if item, exists := c.items[key]; exists {
		switch item.value.(type) {
		case string:
			retType = "string"
		case restArray:
			retType = "list"
		}
	}
	return fmt.Sprintf("+%s\r\n", retType)
}

// --- Cache clear ---

func (c *Cache) reapLoop() {
	ticker := time.NewTicker(c.interval)
	// defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()

		for key, item := range c.items {
			if item.isExpired() {
				delete(c.items, key)
			}
		}
		defer c.mu.Unlock()
	}
}

func (i item) isExpired() bool {
	if i.expiry.IsZero() {
		return false
	}
	return time.Now().After(i.expiry)
}

func logArray(head *restArrayElement) {
	for curr := head; curr != nil; curr = curr.next {
		log.Printf("%s", curr.data)
	}
}
