package drop

import (
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEmpty(t *testing.T) {
	server := NewManager(5, "/tmp")
	paths, err := server.Request(Drop{"23"})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Errorf("Non-empty result returned. Len: %v", len(paths))
	}
	server.clean()
}

type testParam struct {
	dropId         string
	numSub, numGot int
}

func submitAndCheck(t *testing.T, server *DropManager, data []testParam) {
	since := time.Now()
	// submit
	for _, req := range data {
		for i := 0; i < req.numSub; i++ {
			err := server.Submit(Drop{req.dropId}, strings.NewReader(req.dropId+strconv.Itoa(i)))
			if err != nil {
				t.Error(err)
			}
		}
	}
	// request
	for _, req := range data {
		paths, err := server.RequestSince(Drop{req.dropId}, since)
		if err != nil {
			t.Error(err)
		}
		if len(paths) != req.numGot {
			t.Errorf("Wrong number of messages returned: %i instead of %v", len(paths), req.numGot)
		}
		// verfiy data
		for i, path := range paths {
			readDataRaw, err := ioutil.ReadFile(path)
			if err != nil {
				t.Error(err)
			}
			readData := string(readDataRaw)
			expectedData := req.dropId + strconv.Itoa(i+req.numSub-req.numGot)
			if readData != expectedData {
				t.Errorf("Read data differs. Read: %v Expected: %v", readData, expectedData)
			}
		}
	}
}

func TestAlmostFull(t *testing.T) {
	params := []testParam{
		{"foo", 3, 3},
		{"bar", 1, 1},
		{"nil", 0, 0},
	}
	server := NewManager(5, "/tmp")
	submitAndCheck(t, &server, params)
	discarded, err := server.clean()
	if err != nil {
		t.Error(err)
	}
	if discarded != 4 {
		t.Errorf("Failed to cleanup all message. Discarded %v.", discarded)
	}
}

func TestOverfill(t *testing.T) {
	params := []testParam{
		{"foo", 3, 2},
		{"bar", 1, 1},
	}
	server := NewManager(3, "/tmp")
	submitAndCheck(t, &server, params)
	discarded, err := server.clean()
	if err != nil {
		t.Error(err)
	}
	if discarded != 3 {
		t.Errorf("Failed to cleanup all message. Discarded %v.", discarded)
	}
}

func TestTwoPass(t *testing.T) {
	params1 := []testParam{
		{"foo", 1, 1},
		{"bar", 1, 1},
	}
	params2 := []testParam{
		{"foo", 2, 2},
		{"bar", 0, 0},
	}
	server := NewManager(4, "/tmp")
	submitAndCheck(t, &server, params1)
	// second request should not return the message submitted in pass one due to timestamp
	submitAndCheck(t, &server, params2)
	discarded, err := server.clean()
	if err != nil {
		t.Error(err)
	}
	if discarded != 4 {
		t.Errorf("Failed to cleanup all message. Discarded %v.", discarded)
	}
}

type mockReader struct {
	toRead int
}

func (m *mockReader) Read(p []byte) (int, error) {
	// don't actually deliver any data - size matters!
	if m.toRead == 0 {
		return 0, io.EOF
	}
	var nowRead int
	if m.toRead > cap(p) {
		nowRead = cap(p)
	} else {
		nowRead = m.toRead
	}
	m.toRead -= nowRead
	return nowRead, nil
}

func TestMsgSizeOk(t *testing.T) {
	server := NewManager(2, "/tmp")
	err := server.Submit(Drop{"foo"}, &mockReader{MaximumMessageSize})
	if err != nil {
		t.Error(err)
	}
}

func TestMsgSizeTooLarge(t *testing.T) {
	server := NewManager(2, "/tmp")
	err := server.Submit(Drop{"foo"}, &mockReader{MaximumMessageSize + 1})
	if _, ok := err.(MessageSizeExceededError); !ok {
		if err != nil {
			t.Error(err)
		}
		t.Error("Server should have rejected too large message")
	}
}

func TestDropIdVerification(t *testing.T) {
	goodDrop := Drop{"abcdefghijklmnopqrstuvwxyzabcdefgworkingUrl"}
	if !goodDrop.Verify() {
		t.Error("failed to verify good id")
	}
	badShortDrop := Drop{"tooshort"}
	if badShortDrop.Verify() {
		t.Error("Failed to detect too short id")
	}
}
