package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math"
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
	items       map[string]*item
	mu          sync.Mutex
	interval    time.Duration
	popQueue    map[string]*consumerQueue
	streamQueue map[string]*xreadConsumerQueue
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

type xreadConsumer struct {
	next *xreadConsumer
	prev *xreadConsumer
	ch   chan struct{}
}

type xreadConsumerQueue struct {
	head  *xreadConsumer
	tail  *xreadConsumer
	count int
}

type stream struct {
	head  *streamEntry
	tail  *streamEntry
	count int
}

type streamEntry struct {
	msTime time.Time
	seqNum int64
	count  int
	kv     map[string]string
	next   *streamEntry
	prev   *streamEntry
}

type ququeCmd struct {
	name string
	args []string
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	cache := &Cache{
		items:       make(map[string]*item),
		interval:    time.Hour,
		popQueue:    make(map[string]*consumerQueue),
		streamQueue: make(map[string]*xreadConsumerQueue),
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
		"XADD":   cache.cmdXadd,
		"XRANGE": cache.cmdXrange,
		"XREAD":  cache.cmdXread,
		"INCR":   cache.cmdIncr,
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
	var inMulti bool
	var txQueue []ququeCmd
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		name := strings.ToUpper(args[0])
		if name == "MULTI" {
			if inMulti {
				conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
				continue
			}
			inMulti = true
			conn.Write([]byte("+OK\r\n"))
			continue
		}
		if name == "DISCARD" {
			if !inMulti {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
				continue
			}
			inMulti = false
			txQueue = nil
			conn.Write([]byte("+OK\r\n"))
			continue
		}
		if name == "EXEC" {
			if !inMulti {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}
			inMulti = false
			var sb strings.Builder
			fmt.Fprintf(&sb, "*%d\r\n", len(txQueue))
			for _, cmd := range txQueue {
				h, ok := commands[cmd.name]
				if ok {
					sb.WriteString(h(cmd.args))
				} else {
					sb.WriteString("-ERR unknown command '" + cmd.name + "'\r\n")
				}
			}
			txQueue = nil
			conn.Write([]byte(sb.String()))
			continue
		}
		if inMulti {
			txQueue = append(txQueue, ququeCmd{name: name, args: args[1:]})
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}
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
		case *restArray:
			retType = "list"
		case *stream:
			retType = "stream"
		}
	}
	return fmt.Sprintf("+%s\r\n", retType)
}

func (c *Cache) cmdXadd(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'xadd' command\r\n"
	}

	streamKey := args[0]
	idStr := args[1]
	if args[1] == "*" {
		idStr = "*-*"
	}
	streamId := strings.Split(idStr, "-")
	if len(streamId) != 2 {
		return "-ERR wrong id format\r\n"
	}
	if args[1] == "0-0" {
		return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
	}

	newEntry := &streamEntry{
		kv: make(map[string]string),
	}

	if streamId[0] != "*" {
		timeInt, err := strconv.ParseInt(streamId[0], 10, 64)
		if err != nil {
			return "-ERR\r\n"
		}
		newEntry.msTime = time.UnixMilli(timeInt)
	} else {
		newEntry.msTime = time.Now()
	}

	if streamId[1] != "*" {
		seqInt, err := strconv.ParseInt(streamId[1], 10, 64)
		if err != nil {
			return "-ERR\r\n"
		}
		newEntry.seqNum = seqInt
	} else if newEntry.msTime.UnixMilli() == 0 {
		newEntry.seqNum = 1
	}

	// log.Print(args[2:])
	for i := 2; i < len(args); i += 2 {
		newEntry.kv[args[i]] = args[i+1]
		newEntry.count++
	}

	// log.Printf("%#v\n%#v", newEntry, newEntry.kv)

	c.mu.Lock()
	defer c.mu.Unlock()
	if i, exists := c.items[streamKey]; exists {
		if stream, ok := i.value.(*stream); ok {
			prevEntry := stream.tail
			prevMsTime := prevEntry.msTime
			prevSeq := prevEntry.seqNum
			if newEntry.msTime.Before(prevMsTime) {
				return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
			} else if newEntry.msTime.Equal(prevMsTime) && newEntry.seqNum <= prevSeq {
				if streamId[1] == "*" {
					newEntry.seqNum = prevSeq + 1
				} else {
					return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
				}
			}
			newEntry.prev = stream.tail
			stream.tail.next = newEntry
			stream.tail = newEntry
			stream.count++
		} else {
			return "-ERR not steram\r\n"
		}
	} else {
		newStream := &stream{
			head:  newEntry,
			tail:  newEntry,
			count: 1,
		}
		newItem := item{
			value:  newStream,
			expiry: time.Time{},
		}
		c.items[streamKey] = &newItem
	}
	if queue, exists := c.streamQueue[streamKey]; exists {
		for nc := queue.head; nc != nil; nc = nc.next {
			select {
			case nc.ch <- struct{}{}:
			default:
			}
		}
	}
	ss := fmt.Sprintf("%d-%d", newEntry.msTime.UnixMilli(), newEntry.seqNum)
	return fmt.Sprintf("$%d\r\n%s\r\n", len(ss), ss)
}

func (c *Cache) cmdXrange(args []string) string {
	if len(args) != 3 {
		return "-ERR wrong number of arguments for 'type' command"
	}

	startId, err := parseId(args[1])
	if err != nil {
		return err.Error()
	}

	endId, err := parseId(args[2])
	if err != nil {
		return err.Error()
	}

	if startId.msTime.After(endId.msTime) {
		return "-ERR\r\n"
	}

	if endId.seqNr == -1 {
		endId.seqNr = math.MaxInt64
	}

	key := args[0]

	c.mu.Lock()
	defer c.mu.Unlock()

	if i, exists := c.items[key]; exists {
		if stream, ok := i.value.(*stream); ok {
			var respArray strings.Builder
			var count int
			for cur := stream.head; cur != nil; cur = cur.next {
				if cur.msTime.After(endId.msTime) || (cur.msTime.Equal(endId.msTime) && cur.seqNum > endId.seqNr) {
					break
				}
				if cur.msTime.Before(startId.msTime) || (cur.msTime.Equal(startId.msTime) && cur.seqNum < startId.seqNr) {
					continue
				}
				fmt.Fprintf(&respArray, "*2\r\n")
				id := fmt.Sprintf("%d-%d", cur.msTime.UnixMilli(), cur.seqNum)
				fmt.Fprintf(&respArray, "$%d\r\n%s\r\n", len(id), id)
				fmt.Fprintf(&respArray, "*%d\r\n", cur.count*2)
				for k, v := range cur.kv {
					fmt.Fprintf(&respArray, "$%d\r\n%s\r\n", len(k), k)
					fmt.Fprintf(&respArray, "$%d\r\n%s\r\n", len(v), v)
				}
				count++
			}
			return fmt.Sprintf("*%d\r\n%s", count, respArray.String())
		}
	}

	return "*0\r\n"
}

func (c *Cache) xreadEntries(keys, ids []string) string {
	var result strings.Builder
	count := 0
	for i, key := range keys {
		startId, err := parseId(ids[i])
		if err != nil {
			continue
		}
		it, exists := c.items[key]
		if !exists {
			continue
		}
		s, ok := it.value.(*stream)
		if !ok {
			continue
		}
		var entries strings.Builder
		entryCount := 0
		for cur := s.head; cur != nil; cur = cur.next {
			if cur.msTime.Before(startId.msTime) || (cur.msTime.Equal(startId.msTime) && cur.seqNum <= startId.seqNr) {
				continue
			}
			id := fmt.Sprintf("%d-%d", cur.msTime.UnixMilli(), cur.seqNum)
			fmt.Fprintf(&entries, "*2\r\n$%d\r\n%s\r\n*%d\r\n", len(id), id, cur.count*2)
			for k, v := range cur.kv {
				fmt.Fprintf(&entries, "$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
			}
			entryCount++
		}
		if entryCount == 0 {
			continue
		}
		fmt.Fprintf(&result, "*2\r\n$%d\r\n%s\r\n*%d\r\n%s", len(key), key, entryCount, entries.String())
		count++
	}
	if count == 0 {
		return ""
	}
	return fmt.Sprintf("*%d\r\n%s", count, result.String())
}

func (c *Cache) cmdXread(args []string) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments\r\n"
	}

	idx := 0
	blockMs := int64(-1)

	if strings.ToUpper(args[idx]) == "BLOCK" {
		idx++
		if idx >= len(args) {
			return "-ERR syntax error\r\n"
		}
		ms, err := strconv.ParseInt(args[idx], 10, 64)
		if err != nil {
			return "-ERR invalid BLOCK timeout\r\n"
		}
		blockMs = ms
		idx++
	}

	if idx >= len(args) || strings.ToUpper(args[idx]) != "STREAMS" {
		return "-ERR syntax error\r\n"
	}
	idx++

	rest := args[idx:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return "-ERR syntax error\r\n"
	}
	half := len(rest) / 2
	keys := rest[:half]
	ids := rest[half:]

	c.mu.Lock()

	resolvedIds := make([]string, len(ids))
	for i, id := range ids {
		if id == "$" {
			resolvedIds[i] = "0-0"
			if it, exists := c.items[keys[i]]; exists {
				if s, ok := it.value.(*stream); ok && s.tail != nil {
					resolvedIds[i] = fmt.Sprintf("%d-%d", s.tail.msTime.UnixMilli(), s.tail.seqNum)
				}
			}
		} else {
			resolvedIds[i] = id
		}
	}

	if blockMs < 0 {
		result := c.xreadEntries(keys, resolvedIds)
		c.mu.Unlock()
		if result == "" {
			return "*-1\r\n"
		}
		return result
	}

	result := c.xreadEntries(keys, resolvedIds)
	if result != "" {
		c.mu.Unlock()
		return result
	}

	sharedCh := make(chan struct{}, len(keys))
	consumers := make([]*xreadConsumer, len(keys))
	for i, key := range keys {
		nc := &xreadConsumer{ch: sharedCh}
		consumers[i] = nc
		if queue, exists := c.streamQueue[key]; exists {
			nc.prev = queue.tail
			queue.tail.next = nc
			queue.tail = nc
			queue.count++
		} else {
			c.streamQueue[key] = &xreadConsumerQueue{head: nc, tail: nc, count: 1}
		}
	}

	c.mu.Unlock()

	var timeout <-chan time.Time
	if blockMs > 0 {
		timeout = time.After(time.Duration(blockMs) * time.Millisecond)
	}

	var retString string
	select {
	case <-sharedCh:
		c.mu.Lock()
		retString = c.xreadEntries(keys, resolvedIds)
		c.mu.Unlock()
		if retString == "" {
			retString = "*-1\r\n"
		}
	case <-timeout:
		retString = "*-1\r\n"
	}

	c.mu.Lock()
	for i, key := range keys {
		nc := consumers[i]
		queue := c.streamQueue[key]
		if queue == nil {
			continue
		}
		queue.count--
		if queue.count == 0 {
			delete(c.streamQueue, key)
			continue
		}
		if queue.head == nc {
			queue.head = nc.next
			if queue.head != nil {
				queue.head.prev = nil
			}
		} else if queue.tail == nc {
			queue.tail = nc.prev
			if queue.tail != nil {
				queue.tail.next = nil
			}
		} else {
			nc.prev.next = nc.next
			nc.next.prev = nc.prev
		}
	}
	c.mu.Unlock()

	return retString
}

func (c *Cache) cmdIncr(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments\r\n"
	}

	key := args[0]
	if i, exists := c.items[key]; exists {
		if str, ok := i.value.(string); ok {
			val, err := strconv.ParseInt(str, 10, 64)
			if err == nil {
				val++
				c.items[key] = &item{
					value:  strconv.FormatInt(val, 10),
					expiry: i.expiry,
				}
				return fmt.Sprintf(":%d\r\n", val)
			}
			// log.Print(val)
		}
		return "-ERR value is not an integer or out of range\r\n"
	} else {
		c.items[key] = &item{
			value:  "1",
			expiry: time.Time{},
		}
		return ":1\r\n"
	}
	return "-ERR\r\n"
}

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

type streamId struct {
	msTime time.Time
	seqNr  int64
}

func parseId(id string) (streamId, error) {
	if id == "-" {
		return streamId{
			msTime: time.Time{},
			seqNr:  0,
		}, nil
	}
	if id == "+" {
		return streamId{
			msTime: time.UnixMilli(math.MaxInt64),
			seqNr:  math.MaxInt64,
		}, nil
	}
	var stream streamId
	idParts := strings.Split(id, "-")
	msInt, err := strconv.ParseInt(idParts[0], 10, 64)
	if err != nil {
		return streamId{}, errors.New("-ERR\r\n")
	}
	stream.msTime = time.UnixMilli(msInt)
	stream.seqNr = -1
	if len(idParts) == 2 {
		if stream.seqNr, err = strconv.ParseInt(idParts[1], 10, 64); err != nil {
			return streamId{}, errors.New("-ERR\r\n")
		}
	}
	return stream, nil
}
