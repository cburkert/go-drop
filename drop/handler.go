package drop

import (
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path"
	"strings"
	"time"
)

type dropHandler struct {
	server   DropManager
	baseDir  string
	capacity int
}

func (handler *dropHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cleanedPath := path.Clean(r.URL.Path)
	log.Println(r.Method + " " + cleanedPath)
	pathTokens := strings.Split(cleanedPath, "/")
	drop := Drop{}

	// First token is empty because of heading slash
	switch len(pathTokens) {
	case 2:
		drop.Id = pathTokens[1]
	default:
		http.Error(w, "Invalid request path", http.StatusBadRequest)
		return
	}

	if !drop.Verify() {
		http.Error(w, "Invalid drop ID", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		var paths []string
		var reqErr error
		since, convErr := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since"))
		if convErr == nil {
			paths, reqErr = handler.server.RequestSince(drop, since)
		} else {
			paths, reqErr = handler.server.Request(drop)
		}
		if reqErr != nil {
			log.Print(reqErr)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		if len(paths) == 0 {
			if convErr == nil {
				http.Error(w, http.StatusText(http.StatusNotModified), http.StatusNotModified)
				return
			} else {
				http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
				return
			}
		}
		w.Header().Set("Content-Type", "multipart/mixed")
		w.WriteHeader(http.StatusOK)
		mpWriter := multipart.NewWriter(w)
		for _, msgPath := range paths {
			part, err := mpWriter.CreatePart(textproto.MIMEHeader{})
			if err != nil {
				log.Print(err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
			file, err := os.Open(msgPath)
			if err != nil {
				log.Print(err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
			defer file.Close()
			_, err = io.CopyN(part, file, MaximumMessageSize)
			if err != nil && err != io.EOF {
				log.Print(err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
		}
		mpWriter.Close()
		return
	case "POST":
		err := handler.server.Submit(drop, r.Body)
		if err != nil {
			if sizeErr, ok := err.(MessageSizeExceededError); ok {
				http.Error(w, sizeErr.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			log.Print(err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		http.Error(w, "Successful", http.StatusOK)
		return
	default:
		log.Fatal("Unexpected request")
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
}

func DropServer(capacity int, baseDir string) http.Handler {
	return &dropHandler{
		baseDir:  baseDir,
		capacity: capacity,
		server:   NewManager(capacity, baseDir),
	}
}
