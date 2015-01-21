package drop

import (
	"container/ring"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"
)

const (
	maximumDropMessages = 32
	maxMsgSize          = 4096
)

type Drop struct {
	Id string
}

type Message struct {
	Drop      Drop
	Timestamp time.Time
	Path      string
}

func (msg *Message) discard() error {
	return os.Remove(msg.Path)
}

type DropServer struct {
	Capacity   int
	BaseDir    string
	MsgRing    *ring.Ring
	SubmitChan chan Message
}

func NewServer(capacity int, baseDir string) DropServer {
	msgRing := ring.New(capacity)
	return DropServer{
		Capacity:   capacity,
		BaseDir:    baseDir,
		MsgRing:    msgRing,
		SubmitChan: launchRingManager(msgRing),
	}
}

func launchRingManager(msgRing *ring.Ring) chan Message {
	submitChan := make(chan Message)
	go func() {
		for {
			msg := <-submitChan
			oldMsg, oldExisted := msgRing.Value.(Message)
			msgRing.Value = msg
			if oldExisted {
				err := oldMsg.discard()
				if err != nil {
					log.Panic("Failed to discard message")
				}
			}
			msgRing = msgRing.Next()
		}
	}()
	return submitChan
}

func (server *DropServer) submit(drop Drop, data io.Reader) error {
	msg := Message{
		Drop:      drop,
		Timestamp: time.Now(),
	}
	file, err := ioutil.TempFile(server.BaseDir, "")
	if err != nil {
		return err
	}
	defer file.Close()
	msg.Path = file.Name()
	_, err = io.CopyN(file, data, maxMsgSize+1)
	if err != io.EOF {
		os.Remove(msg.Path)
		if err == nil {
			return errors.New("Maximum drop size exceeded")
		} else {
			// some other unforeseen error occurred
			return err
		}
	}
	server.SubmitChan <- msg

	return nil
}

func (server *DropServer) request(drop Drop, since time.Time) ([]string, error) {
	paths := make([]string, 0)
	server.MsgRing.Do(func(value interface{}) {
		msg, ok := value.(Message)
		if ok && msg.Drop.Id == drop.Id && msg.Timestamp.After(since) {
			paths = append(paths, msg.Path)
		}
	})
	return paths, nil
}

func (server *DropServer) cleanOlder(newEventHorizon *time.Time) (int, error) {
	numDiscarded := 0
	msgRing := server.MsgRing
	first := true
	for ; first || msgRing != server.MsgRing; msgRing = msgRing.Next() {
		first = false
		msg, ok := msgRing.Value.(Message)
		if ok && (newEventHorizon == nil || msg.Timestamp.Before(*newEventHorizon)) {
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

func (server *DropServer) clean() (int, error) {
	return server.cleanOlder(nil)
}
