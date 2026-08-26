// dashboard.go exposes a per-game /api/stats JSON for the unified Nextendo monitoring site,
// same schema every other title in this codebase uses (see arms/dashboard.go) so the existing
// aggregator UI renders it unchanged. Odyssey has no matchmaking/lobbies, so "gatherings" is
// always empty here; "balloons" carries the equivalent live-activity view (recent placements)
// instead.
package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

var (
	dashStart = time.Now()

	metaMu        sync.Mutex
	playerMeta    = map[uint64]*playerInfo{}
	sessionsSeen  int64
	peakConnected int

	rmcTotal int64

	eventsMu    sync.Mutex
	events      []rmcEvent
	methodCount = map[string]int64{}
)

type playerInfo struct {
	PID        uint64
	FirstSeen  time.Time
	LastSeen   time.Time
	IP         string
	Calls      int64
	LastProto  uint16
	LastMethod uint32
}

type rmcEvent struct {
	T      time.Time
	PID    uint64
	Proto  uint16
	Method uint32
}

func noteRMC(c *nex.Connection, req *nex.RMCMessage) {
	pid := c.PID
	if pid == 0 {
		return
	}
	atomic.AddInt64(&rmcTotal, 1)

	metaMu.Lock()
	pi := playerMeta[pid]
	if pi == nil {
		pi = &playerInfo{PID: pid, FirstSeen: time.Now()}
		playerMeta[pid] = pi
		sessionsSeen++
	}
	pi.LastSeen = time.Now()
	pi.Calls++
	pi.LastProto = req.Protocol
	pi.LastMethod = req.Method
	if c.RemoteAddr != "" {
		pi.IP = c.RemoteAddr
	}
	metaMu.Unlock()

	eventsMu.Lock()
	events = append(events, rmcEvent{T: time.Now(), PID: pid, Proto: req.Protocol, Method: req.Method})
	if len(events) > 100 {
		events = events[len(events)-100:]
	}
	methodCount[rmcName(req.Protocol, req.Method)]++
	eventsMu.Unlock()
}

func rmcName(proto uint16, method uint32) string {
	pn := map[uint16]string{
		0x0A: "TicketGranting", 0x0B: "SecureConnection", 0x6E: "Utility",
		0x70: "Ranking", 0x73: "DataStore",
	}[proto]
	if pn == "" {
		pn = fmt.Sprintf("Proto-0x%X", proto)
	}
	mn := ""
	switch proto {
	case 0x0A:
		mn = map[uint32]string{1: "Login", 2: "LoginEx", 3: "RequestTicket"}[method]
	case 0x0B:
		mn = map[uint32]string{1: "Register", 4: "RegisterEx", 7: "ReplaceURL"}[method]
	case 0x6E:
		mn = map[uint32]string{1: "AcquireNexUniqueID", 7: "GetIntegerSettings", 8: "GetStringSettings"}[method]
	case 0x70:
		mn = map[uint32]string{1: "UploadScore", 4: "UploadCommonData", 6: "GetCommonData"}[method]
	case 0x73:
		mn = map[uint32]string{
			4: "DeleteObject", 8: "GetMeta", 9: "GetMetas", 12: "SearchObject",
			15: "RateObject", 16: "GetRating", 24: "PreparePostObject", 25: "PrepareGetObject",
			26: "CompletePostObject", 47: "AddToBufferQueue", 48: "AddToBufferQueues",
			49: "GetBufferQueue", 50: "GetBufferQueues", 51: "ClearBufferQueues",
			52: "SearchBalloon", 53: "FetchMyInfos",
		}[method]
	}
	if mn == "" {
		mn = fmt.Sprintf("m%d", method)
	}
	return pn + "::" + mn
}

// ----- stats JSON (keys mirror the shared aggregator schema) --------------------

type apiPlayer struct {
	PID        uint64 `json:"pid"`
	Name       string `json:"name"`
	IP         string `json:"ip"`
	State      string `json:"state"`
	Gathering  uint32 `json:"gathering"`
	OnlineSecs int    `json:"onlineSeconds"`
	Calls      int64  `json:"calls"`
	LastAction string `json:"lastAction"`
	IdleSecs   int    `json:"idleSeconds"`
}

type apiEvent struct {
	Ago    int    `json:"agoSeconds"`
	PID    uint64 `json:"pid"`
	Action string `json:"action"`
}

type apiMethod struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type apiServer struct {
	AccessKey  string `json:"accessKey"`
	NexVersion string `json:"nexVersion"`
	AuthPort   string `json:"authPort"`
	SecurePort int    `json:"securePort"`
	SNIHost    string `json:"sniHost"`
	SessionKey int    `json:"sessionKeyLen"`
	Stack      string `json:"stack"`
}

// apiBalloon is Odyssey's stand-in for the "gatherings" panel other titles show: a snapshot
// of recently-placed balloons instead of live lobbies, since Odyssey has none.
type apiBalloon struct {
	DataID    uint64 `json:"dataId"`
	OwnerPID  uint64 `json:"ownerPid"`
	Name      string `json:"name"`
	DataType  uint16 `json:"dataType"`
	AgeSecs   int    `json:"ageSeconds"`
	RatingSum int64  `json:"ratingSum"`
	RatingN   uint32 `json:"ratingN"`
}

type apiStats struct {
	ServerTime    string       `json:"serverTime"`
	UptimeSeconds int          `json:"uptimeSeconds"`
	Connected     int          `json:"connected"`
	InLobby       int          `json:"inLobby"`
	ActiveLobbies int          `json:"activeLobbies"`
	TotalSessions int64        `json:"totalSessions"`
	TotalRMC      int64        `json:"totalRmc"`
	TotalBalloons int64        `json:"gatheringsMade"`
	PeakConnected int          `json:"peakConnected"`
	Server        apiServer    `json:"server"`
	Players       []apiPlayer  `json:"players"`
	Gatherings    []struct{}   `json:"gatherings"`
	Balloons      []apiBalloon `json:"balloons"`
	Events        []apiEvent   `json:"events"`
	Methods       []apiMethod  `json:"methods"`
}

// dispName resolves a real name via nextendo-account, falling back to a placeholder.
const nameCacheTTL = 60 * time.Second

type nameCacheEntry struct {
	name string
	at   time.Time
}

var (
	nameCacheMu sync.Mutex
	nameCache   = map[uint64]nameCacheEntry{}
)

func dispName(pid uint64) string {
	nameCacheMu.Lock()
	if e, ok := nameCache[pid]; ok && time.Since(e.at) < nameCacheTTL {
		nameCacheMu.Unlock()
		return e.name
	}
	nameCacheMu.Unlock()

	name := fmt.Sprintf("Player-%d", pid%100000)
	if resp, err := gateClient.Get(fmt.Sprintf("%s/api/names?pids=%d", accountBaseURL, pid)); err == nil {
		var out struct {
			Names map[string]struct {
				Name string `json:"name"`
			} `json:"names"`
		}
		if json.NewDecoder(resp.Body).Decode(&out) == nil {
			if e, ok := out.Names[strconv.FormatUint(pid, 10)]; ok && e.Name != "" {
				name = e.Name
			}
		}
		resp.Body.Close()
	}

	nameCacheMu.Lock()
	nameCache[pid] = nameCacheEntry{name: name, at: time.Now()}
	nameCacheMu.Unlock()
	return name
}

func nexVersionString() string {
	return fmt.Sprintf("%d.%d.%d", nexVersion/10000, (nexVersion/100)%100, nexVersion%100)
}

func buildStats(endpoint *nex.Endpoint) apiStats {
	conns := endpoint.SnapshotConnections()

	type metaSnap struct {
		calls       int64
		first, last time.Time
		proto       uint16
		meth        uint32
		ip          string
	}
	metaMu.Lock()
	snap := make(map[uint64]metaSnap, len(playerMeta))
	for pid, pi := range playerMeta {
		snap[pid] = metaSnap{calls: pi.Calls, first: pi.FirstSeen, last: pi.LastSeen, proto: pi.LastProto, meth: pi.LastMethod, ip: pi.IP}
	}
	if len(conns) > peakConnected {
		peakConnected = len(conns)
	}
	peak := peakConnected
	sessions := sessionsSeen
	metaMu.Unlock()

	now := time.Now()
	players := make([]apiPlayer, 0, len(conns))
	seenPID := map[uint64]bool{}
	for _, c := range conns {
		if c.PID == 0 || seenPID[c.PID] {
			continue
		}
		seenPID[c.PID] = true
		m := snap[c.PID]
		players = append(players, apiPlayer{
			PID: c.PID, Name: dispName(c.PID), IP: m.ip, State: "Online",
			OnlineSecs: int(now.Sub(m.first).Seconds()), Calls: m.calls,
			LastAction: rmcName(m.proto, m.meth), IdleSecs: int(now.Sub(m.last).Seconds()),
		})
	}
	sort.Slice(players, func(i, j int) bool { return players[i].OnlineSecs > players[j].OnlineSecs })

	eventsMu.Lock()
	evs := make([]apiEvent, 0, len(events))
	for i := len(events) - 1; i >= 0 && len(evs) < 40; i-- {
		e := events[i]
		evs = append(evs, apiEvent{Ago: int(now.Sub(e.T).Seconds()), PID: e.PID, Action: rmcName(e.Proto, e.Method)})
	}
	methods := make([]apiMethod, 0, len(methodCount))
	for name, cnt := range methodCount {
		methods = append(methods, apiMethod{Name: name, Count: cnt})
	}
	eventsMu.Unlock()
	sort.Slice(methods, func(i, j int) bool { return methods[i].Count > methods[j].Count })

	dsMu.Lock()
	balloons := make([]apiBalloon, 0, len(dsRecords))
	for _, r := range dsRecords {
		if !r.Complete {
			continue
		}
		balloons = append(balloons, apiBalloon{
			DataID: r.DataID, OwnerPID: r.OwnerID, Name: r.Name, DataType: r.DataType,
			AgeSecs:   int(now.Sub(r.CreatedWall).Seconds()),
			RatingSum: r.RatingSum, RatingN: r.RatingN,
		})
	}
	totalBalloons := int64(len(dsRecords))
	dsMu.Unlock()
	sort.Slice(balloons, func(i, j int) bool { return balloons[i].DataID > balloons[j].DataID })
	if len(balloons) > 40 {
		balloons = balloons[:40]
	}

	return apiStats{
		ServerTime: now.UTC().Format(time.RFC3339), UptimeSeconds: int(now.Sub(dashStart).Seconds()),
		Connected: len(players), TotalSessions: sessions, TotalRMC: atomic.LoadInt64(&rmcTotal),
		TotalBalloons: totalBalloons, PeakConnected: peak,
		Server: apiServer{
			AccessKey: accessKey, NexVersion: nexVersionString(), AuthPort: strconv.Itoa(authPort),
			SecurePort: securePort, SNIHost: envOr("SMO_SNI_HOST", ""), SessionKey: sessionKeyLen,
			Stack: "npln-smo",
		},
		Players: players, Gatherings: []struct{}{}, Balloons: balloons, Events: evs, Methods: methods,
	}
}

func startDashboard(endpoint *nex.Endpoint) {
	port := envOr("DASH_PORT", "8089")
	token := envOr("DASH_TOKEN", "")

	authed := func(w http.ResponseWriter, r *http.Request) bool {
		if token != "" && subtle.ConstantTimeCompare([]byte(r.URL.Query().Get("key")), []byte(token)) == 1 {
			return true
		}
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		if !authed(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(buildStats(endpoint))
	})
	mux.HandleFunc("/api/kick", func(w http.ResponseWriter, r *http.Request) {
		if !authed(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if pid, err := strconv.ParseUint(r.URL.Query().Get("pid"), 10, 64); err == nil && pid != 0 {
			_ = json.NewEncoder(w).Encode(map[string]any{"pid": pid, "kicked": endpoint.KickPID(pid)})
			return
		}
		if rv, err := strconv.ParseUint(r.URL.Query().Get("rvcid"), 10, 32); err == nil && rv != 0 {
			_ = json.NewEncoder(w).Encode(map[string]any{"rvcid": rv, "kicked": endpoint.KickConnection(uint32(rv))})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"reaped": endpoint.ReapIdle(nex.ReapIdleTimeout())})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })

	fmt.Printf("[SMO Dashboard] stats API on :%s (token=%v)\n", port, token != "")
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		fmt.Printf("[SMO Dashboard] stopped: %v\n", err)
	}
}
