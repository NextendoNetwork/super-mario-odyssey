// presence.go: same fallback presence mechanism every other Nextendo game server uses (see
// arms/presence.go) -- report active PIDs to nextendo-account every 30s so it keeps them
// ONLINE via its TTL for the friend list, alongside the real nn::friends UpdateUserPresence
// path citron forwards directly.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	presenceInterval = 30 * time.Second
	presenceTTL      = 60 * time.Second
	presenceStatus   = 2 // "playing"
)

var (
	presMu   sync.Mutex
	presSeen = map[uint64]time.Time{}
)

func notePresenceSeen(pid uint64) {
	if pid == 0 {
		return
	}
	presMu.Lock()
	presSeen[pid] = time.Now()
	presMu.Unlock()
}

func activePIDs() []uint64 {
	now := time.Now()
	active := []uint64{}
	presMu.Lock()
	for pid, t := range presSeen {
		if now.Sub(t) < presenceTTL {
			active = append(active, pid)
		} else {
			delete(presSeen, pid)
		}
	}
	presMu.Unlock()
	return active
}

func startPresenceReporter() {
	base := envOr("NEXTENDO_ACCOUNT_URL", "http://nextendo-account:8080")
	internalKey := os.Getenv("NEXTENDO_INTERNAL_KEY")
	client := &http.Client{Timeout: 5 * time.Second}
	go func() {
		for {
			time.Sleep(presenceInterval)
			active := activePIDs()
			if len(active) == 0 {
				continue
			}
			body, err := json.Marshal(map[string]any{"appId": smoTitleID, "status": presenceStatus, "pids": active})
			if err != nil {
				continue
			}
			req, err := http.NewRequest("POST", base+"/internal/presence-batch", bytes.NewReader(body))
			if err != nil {
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Internal-Key", internalKey)
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("[SMO Presence] batch report failed: %v\n", err)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				respBody, _ := io.ReadAll(resp.Body)
				fmt.Printf("[SMO Presence] batch report rejected: HTTP %d: %s\n", resp.StatusCode, respBody)
			}
			resp.Body.Close()
		}
	}()
}
