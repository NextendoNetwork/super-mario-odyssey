// objectstore.go stands in for the S3 bucket real Nintendo servers hand DataStore upload/
// download URLs into. PreparePostObject/PrepareGetObject (datastore.go) issue URLs pointing
// back at this server instead of Amazon; the console then does a plain HTTPS PUT/GET against
// them exactly as it would against S3, using the "X-Object-Token" request header we told it to
// send (DataStoreReqPostInfo/ReqGetInfo's requestHeaders field).
//
// Blobs live as flat files under SMO_BLOB_DIR, named by dataId. There's no S3-style multipart
// upload support -- Balloon World's payloads (a capture-pose screenshot + metadata) are small
// enough that a single PUT body is what's actually observed from every other DataStore-using
// title in this codebase.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const objectTokenTTL = 10 * time.Minute

type objectToken struct {
	dataID      uint64
	requesterID uint64 // PID that asked for this URL -- who to credit with "found" this balloon on download
	upload      bool
	expires     time.Time
}

var (
	blobDir = envOr("SMO_BLOB_DIR", "smo_blobs")

	tokMu  sync.Mutex
	tokens = map[string]objectToken{}
)

func init() {
	_ = os.MkdirAll(blobDir, 0o755)
}

func newObjectToken(dataID, requesterID uint64, upload bool) string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	tok := hex.EncodeToString(b)
	tokMu.Lock()
	tokens[tok] = objectToken{dataID: dataID, requesterID: requesterID, upload: upload, expires: time.Now().Add(objectTokenTTL)}
	// Sweep expired tokens opportunistically rather than running a separate ticker for what's
	// normally a handful of live entries at a time.
	for k, v := range tokens {
		if time.Now().After(v.expires) {
			delete(tokens, k)
		}
	}
	tokMu.Unlock()
	return tok
}

func objectBaseURL() string {
	return fmt.Sprintf("https://%s:%d", nextendoHost, objectPort)
}

func objectUploadURL(dataID uint64) (url, token string) {
	token = newObjectToken(dataID, 0, true)
	return fmt.Sprintf("%s/object/%d", objectBaseURL(), dataID), token
}

// objectDownloadURLFor lets the caller attribute the eventual download to a specific PID
// (dsPrepareGetObject passes conn.PID) so the object store can mark the balloon "found" by the
// right player once the console actually fetches it.
func objectDownloadURLFor(dataID, requesterID uint64) (url, token string) {
	token = newObjectToken(dataID, requesterID, false)
	return fmt.Sprintf("%s/object/%d", objectBaseURL(), dataID), token
}

func objectPath(dataID uint64) string {
	return filepath.Join(blobDir, fmt.Sprintf("%d.bin", dataID))
}

func objectDelete(dataID uint64) {
	_ = os.Remove(objectPath(dataID))
}

func startObjectStore() {
	mux := http.NewServeMux()
	mux.HandleFunc("/object/", func(w http.ResponseWriter, r *http.Request) {
		var dataID uint64
		if _, err := fmt.Sscanf(r.URL.Path, "/object/%d", &dataID); err != nil {
			http.NotFound(w, r)
			return
		}
		tok := r.Header.Get("X-Object-Token")
		tokMu.Lock()
		info, ok := tokens[tok]
		tokMu.Unlock()
		if !ok || info.dataID != dataID || time.Now().After(info.expires) {
			http.Error(w, "invalid or expired token", http.StatusForbidden)
			return
		}

		switch r.Method {
		case http.MethodPut, http.MethodPost:
			if !info.upload {
				http.Error(w, "token is not an upload token", http.StatusForbidden)
				return
			}
			f, err := os.Create(objectPath(dataID))
			if err != nil {
				http.Error(w, "storage error", http.StatusInternalServerError)
				return
			}
			n, err := io.Copy(f, r.Body)
			f.Close()
			if err != nil {
				http.Error(w, "write error", http.StatusInternalServerError)
				return
			}
			fmt.Printf("[SMO ObjectStore] uploaded dataId=%d bytes=%d\n", dataID, n)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if info.upload {
				http.Error(w, "token is not a download token", http.StatusForbidden)
				return
			}
			f, err := os.Open(objectPath(dataID))
			if err != nil {
				http.NotFound(w, r)
				return
			}
			defer f.Close()
			io.Copy(w, f)
			if info.requesterID != 0 {
				dsMu.Lock()
				rec, exists := dsRecords[dataID]
				dsMu.Unlock()
				if exists && rec.OwnerID != info.requesterID {
					markBalloonFound(info.requesterID, dataID)
				}
			}
			fmt.Printf("[SMO ObjectStore] downloaded dataId=%d by pid=%d\n", dataID, info.requesterID)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	fmt.Printf("[SMO ObjectStore] listening HTTPS :%d\n", objectPort)
	if err := http.ListenAndServeTLS(fmt.Sprintf(":%d", objectPort), certFile, keyFile, mux); err != nil {
		fmt.Printf("[SMO ObjectStore] stopped: %v\n", err)
	}
}
