package drop

import (
	"container/ring"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

const (
	MaximumMessageSize = 4096
	dropIdLength       = 32
)

type MessageSizeExceededError struct{}

func (m MessageSizeExceededError) Error() string {
	return fmt.Sprintf("drop: maximum message size of %d bytes exceeded", MaximumMessageSize)
}

type Drop struct {
	Id string
}

func (d Drop) Verify() bool {
	// must be 32 byte encoded to 43 base64url characters
	bytes, err := base64.URLEncoding.DecodeString(d.Id + "=") // add padding to make parser happy
	return err == nil && len(bytes) == dropIdLength
}

type message struct {
	drop      Drop
	timestamp time.Time
	path      string
}

func (msg *message) prefix() string {
	return fmt.Sprintf("%v_", msg.drop.Id)
}

func (msg *message) discard() error {
	return os.Remove(msg.path)
}

type DropManager struct {
	capacity   int
	baseDir    string
	msgRing    *ring.Ring
	submitChan chan message
	mutexRing  sync.Mutex
}

func NewManager(capacity int, baseDir string) DropManager {
	msgRing := ring.New(capacity)
	return DropManager{
		capacity:  capacity,
		baseDir:   baseDir,
		msgRing:   msgRing,
		mutexRing: sync.Mutex{},
	}
}

func (server *DropManager) Submit(drop Drop, data io.Reader) error {
	msg := message{
		drop:      drop,
		timestamp: time.Now(),
	}
	file, err := ioutil.TempFile(server.baseDir, msg.prefix())
	if err != nil {
		return err
	}
	defer file.Close()
	msg.path = file.Name()
	_, err = io.CopyN(file, data, MaximumMessageSize+1)
	if err != io.EOF {
		msg.discard()
		if err == nil {
			return MessageSizeExceededError{}
		} else {
			// some other unforeseen error occurred
			return err
		}
	}

	// update ring
	server.mutexRing.Lock()
	oldMsg, oldExisted := server.msgRing.Value.(message)
	server.msgRing.Value = msg
	server.msgRing = server.msgRing.Next()
	server.mutexRing.Unlock()

	if oldExisted {
		err := oldMsg.discard()
		if err != nil {
			log.Panic("Failed to discard message")
		}
	}

	return nil
}

func (server *DropManager) Request(drop Drop) ([]string, error) {
	return server.handleRequest(drop, false, time.Now())
}

func (server *DropManager) RequestSince(drop Drop, since time.Time) ([]string, error) {
	return server.handleRequest(drop, true, since)
}

func (server *DropManager) handleRequest(drop Drop, filter bool, since time.Time) ([]string, error) {
	paths := make([]string, 0)
	server.msgRing.Do(func(value interface{}) {
		msg, ok := value.(message)
		if ok && msg.drop.Id == drop.Id && (!filter || msg.timestamp.After(since)) {
			paths = append(paths, msg.path)
		}
	})
	return paths, nil
}

func (server *DropManager) cleanOlder(newEventHorizon *time.Time) (int, error) {
	numDiscarded := 0
	msgRing := server.msgRing
	first := true
	for ; first || msgRing != server.msgRing; msgRing = msgRing.Next() {
		first = false
		msg, ok := msgRing.Value.(message)
		if ok && (newEventHorizon == nil || msg.timestamp.Before(*newEventHorizon)) {
			msgRing.Value = nil
			err := msg.discard()
			if err != nil {
				return numDiscarded, err
			}
			numDiscarded++
		}
	}
	return numDiscarded, nil
}

func (server *DropManager) clean() (int, error) {
	return server.cleanOlder(nil)
}
