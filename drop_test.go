package drop

import (
	"io/ioutil"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEmpty(t *testing.T) {
	server := NewServer(5, "/tmp")
	paths, err := server.request(Drop{"23"}, time.Now())
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

func submitAndCheck(t *testing.T, server *DropServer, data []testParam) {
	since := time.Now()
	// submit
	for _, req := range data {
		for i := 0; i < req.numSub; i++ {
			err := server.submit(Drop{req.dropId}, strings.NewReader(req.dropId+strconv.Itoa(i)))
			if err != nil {
				t.Error(err)
			}
		}
	}
	// request
	for _, req := range data {
		paths, err := server.request(Drop{req.dropId}, since)
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
	server := NewServer(5, "/tmp")
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
	server := NewServer(3, "/tmp")
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
	server := NewServer(4, "/tmp")
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
