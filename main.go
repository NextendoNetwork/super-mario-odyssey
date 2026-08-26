// Command super-mario-odyssey runs the Odyssey online servers (auth + secure + object store)
// on the Nextendo NEX stack. Unlike every other title in this codebase, Odyssey has NO
// matchmaking/lobby protocol at all: its entire online feature is Balloon World, which is
// pure DataStore (place a balloon = PreparePostObject, find one = SearchBalloon +
// PrepareGetObject). So this process is simpler than ARMS/MPS/SSBU in one way (no
// Matchmaking/MatchmakeExtension/NATTraversal registration) and more involved in another
// (DataStore has to be a REAL implementation, not a stub -- see datastore.go).
//
// Three servers run in one process:
//   - auth   (:8453) TicketGranting — LoginEx issues the Kerberos ticket, same as every
//     other title.
//   - secure (:60013) SecureConnection + Utility + real DataStore (0x73).
//   - object (:8459) HTTPS blob host standing in for the S3 bucket real Nintendo servers
//     hand DataStore upload/download URLs into (see objectstore.go).
//
// Every value that could not be confirmed on the wire is behind an env var (SMO_*), same
// convention as ARMS/MPS. See example.env and README.md.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"time"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

const (
	// defaultAccessKey / smoGameServerID are Super Mario Odyssey's real NEX identifiers, per
	// the kinnay/NintendoClients wiki's Game Server List (255BA201 / afef0ecf) -- not derived
	// from a binary dump the way ARMS's key was, so treat these as reference-verified rather
	// than independently confirmed against Odyssey's own .rodata.
	defaultAccessKey = "afef0ecf"
	smoGameServerID  = "255BA201"

	// defaultNexVersion: not listed on the Game Server List page (it only has access key +
	// server id). Odyssey shipped Oct 2017, contemporary with Splatoon 2/ACNH/MK8D-era
	// titles, which this codebase's dashboard already records as NEX 4.0.0 -- using the same
	// value here as the best-precedented default until a real capture confirms it.
	defaultNexVersion = 40000

	securePID     = 2
	sessionKeyLen = 32

	// smoTitleID is Super Mario Odyssey's base title ID.
	smoTitleID = "0100000000010000"
)

var (
	accessKey  = envOr("SMO_ACCESS_KEY", defaultAccessKey)
	nexVersion = envOrInt("SMO_NEX_VERSION", defaultNexVersion)

	nextendoHost = envOr("NEXTENDO_HOST", "127.0.0.1")
	authPort     = envOrInt("AUTH_PORT", 8453)
	securePort   = envOrInt("SECURE_PORT", 60013)
	objectPort   = envOrInt("OBJECT_PORT", 8459)

	securePassword = envOr("NEXTENDO_SECURE_PASSWORD", "securepasswordplz1")
	certFile       = envOr("CERT_FILE", "cert.pem")
	keyFile        = envOr("KEY_FILE", "key.pem")

	nextendoSecret = loadNextendoSecret()
	requireAccount = os.Getenv("NEXTENDO_REQUIRE_ACCOUNT") == "1"
)

// stationScheme / secureMinor: same unverified wire-shape knobs every other title in this
// codebase gates behind an env var (see arms/main.go's fuller explanation) -- default to the
// Switch-era "prudps" + minor 0 profile the newer titles (S2/SSBU) need, since Odyssey (like
// them) postdates the MK8-era "prudp"/legacy shape.
func stationScheme() string { return envOr("SMO_STATION_SCHEME", "prudps") }
func secureMinor() int      { return envOrInt("SMO_SECURE_MINOR", 0) }

func main() {
	settings := nex.NewSwitchSettings(accessKey, nexVersion)

	// --- Auth server (:8453) ---
	secureURL := nex.NewStationURL(stationScheme())
	secureURL.Set("address", nextendoHost)
	secureURL.SetInt("port", securePort)
	secureURL.SetInt("CID", 1)
	secureURL.SetInt("PID", securePID)
	secureURL.SetInt("sid", 1)
	secureURL.SetInt("stream", 10)
	secureURL.SetInt("type", 2) // public

	authEndpoint := nex.NewEndpoint(settings)
	authCfg := &nex.AuthConfig{
		Settings:         settings,
		SecurePID:        securePID,
		SecurePassword:   securePassword,
		SecureStationURL: secureURL,
		ServerName:       "Nextendo",
		SessionKeyLength: sessionKeyLen,
		ResolveUser:      resolveUser,
	}
	authEndpoint.Register(nex.ProtocolTicketGranting, authCfg.Handler())
	authEndpoint.OnRMC = logRMC("Auth")
	authServer := nex.NewServer(authEndpoint)

	// --- Secure server (:60013) ---
	secureSettings := nex.NewSwitchSettings(accessKey, nexVersion)
	secureSettings.PrudpMinorVersion = secureMinor()
	secureEndpoint := nex.NewEndpoint(secureSettings)
	secureEndpoint.SetSecureAccount(securePassword, securePID)

	secureEndpoint.Register(nex.ProtocolSecureConnection, nex.SecureConnectionHandler())
	secureEndpoint.Register(nex.ProtocolUtility, nex.UtilityHandler())
	// Ranking (0x70) costs three lines and is harmless to register even if Odyssey's client
	// never links a RankingProtocolClient -- same call ARMS's main.go makes for the same
	// reason. DataStore (0x73) is the real implementation, not a stub -- see datastore.go.
	secureEndpoint.Register(nex.ProtocolRanking, nex.RankingHandler())
	secureEndpoint.Register(protocolDataStore, DataStoreHandler())

	logSecure := logRMC("Secure")
	secureEndpoint.OnRMC = func(c *nex.Connection, req *nex.RMCMessage) {
		logSecure(c, req)
		noteRMC(c, req)
		notePresenceSeen(c.PID)
	}
	secureEndpoint.OnConnect = func(c *nex.Connection) {
		fmt.Printf("[SMO Secure] connected pid=%d id=%d addr=%s\n", c.PID, c.ID, c.RemoteAddr)
	}
	secureServer := nex.NewServer(secureEndpoint)

	secureEndpoint.StartReaper()
	dsLoad()
	dsFlusher()
	go startDashboard(secureEndpoint)
	go startObjectStore()
	startPresenceReporter()

	proxyProto := os.Getenv("NEXTENDO_PROXY_PROTOCOL") == "1"
	go func() {
		fmt.Printf("[SMO Auth] listening WSS :%d (proxyProto=%v, secure URL -> %s)\n", authPort, proxyProto, secureURL.String())
		var err error
		if proxyProto {
			err = authServer.ListenSecureProxy(authPort, certFile, keyFile)
		} else {
			err = authServer.ListenSecure(authPort, certFile, keyFile)
		}
		if err != nil {
			fmt.Printf("[SMO Auth] stopped: %v\n", err)
		}
	}()

	fmt.Printf("[SMO Secure] listening WSS :%d (accessKey=%s nexVersion=%d scheme=%s minor=%d title=%s gameServerId=%s)\n",
		securePort, accessKey, nexVersion, stationScheme(), secureMinor(), smoTitleID, smoGameServerID)
	if err := secureServer.ListenSecure(securePort, certFile, keyFile); err != nil {
		fmt.Printf("[SMO Secure] stopped: %v\n", err)
	}
}

// resolveUser: identical identity/gate logic to every other Nextendo game server in this
// codebase (see arms/main.go's fuller comments) -- signed nx2 token, then bare test PID
// (1800000000-1810000000, real Switch NSA resolution above that), then anonymous fallback.
func resolveUser(username string, _ []byte) (uint64, []byte, bool) {
	sk := sha256.Sum256([]byte("nextendo-src:" + username))
	sourceKey := sk[:]

	if pid, ok := nextendoPIDFromToken(username); ok {
		if allow, reason := nextendoOnlineCheck(pid, "ryujinx"); !allow {
			fmt.Printf("[Auth] pid=%d online REFUSED (%s)\n", pid, reason)
			return 0, nil, false
		}
		return pid, sourceKey, true
	}

	if n, err := strconv.ParseUint(username, 10, 64); err == nil && n >= 1800000000 {
		if requireSignedToken() {
			fmt.Printf("[Auth] pid=%d REFUSED: bare-PID identity disabled (signed nx2 token required)\n", n)
			return 0, nil, false
		}
		fmt.Printf("[Auth] pid=%d bare-PID identity (unauthenticated -- see NEXTENDO_REQUIRE_SIGNED_TOKEN)\n", n)
		pid, kind := n, "ryujinx"
		if n >= 1810000000 {
			kind = "switch"
			rp, st := resolveNSAtoPID(n)
			switch st {
			case nsaOK:
				pid = rp
				fmt.Printf("[Auth] NSA %d -> account pid=%d\n", n, pid)
			case nsaUnknown:
				fmt.Printf("[Auth] NSA %d REFUSED (no Nextendo account)\n", n)
				return 0, nil, false
			case nsaUnreachable:
				fmt.Printf("[Auth] NSA %d REFUSED (account server unreachable)\n", n)
				return 0, nil, false
			}
			if allow, reason := nextendoOnlineCheck(pid, kind); !allow {
				fmt.Printf("[Auth] pid=%d online REFUSED (%s)\n", pid, reason)
				return 0, nil, false
			}
		}
		// bare "ryujinx" test-PIDs stay exempt from online-check: they're never registered accounts
		return pid, sourceKey, true
	}

	if requireAccount {
		fmt.Printf("[Auth] anonymous login REFUSED (Nextendo account required): %q\n", username)
		return 0, nil, false
	}
	return anonymousPID(username), sourceKey, true
}

func nextendoPIDFromToken(s string) (uint64, bool) {
	if len(nextendoSecret) == 0 || !strings.HasPrefix(s, "nx2.") {
		return 0, false
	}
	parts := strings.Split(s[len("nx2."):], ".")
	if len(parts) != 2 {
		return 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return 0, false
	}
	mac := hmac.New(sha256.New, nextendoSecret)
	mac.Write([]byte("nex:" + string(raw)))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(parts[1])) {
		return 0, false
	}
	f := strings.SplitN(string(raw), ".", 3)
	if len(f) != 3 {
		return 0, false
	}
	pid, err := strconv.ParseUint(f[0], 10, 64)
	if err != nil {
		return 0, false
	}
	if exp, err := strconv.ParseInt(f[2], 10, 64); err != nil || time.Now().Unix() > exp {
		return 0, false
	}
	return pid, true
}

func loadNextendoSecret() []byte {
	if v := os.Getenv("NEXTENDO_SECRET"); v != "" {
		return []byte(v)
	}
	path := envOr("NEXTENDO_SECRET_FILE", "nextendo_secret.key")
	if b, err := os.ReadFile(path); err == nil {
		if dec, derr := hex.DecodeString(strings.TrimSpace(string(b))); derr == nil && len(dec) >= 16 {
			return dec
		}
	}
	return nil
}

func anonymousPID(username string) uint64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(username))
	return 1800000000 + uint64(h.Sum32()%100000000)
}

func logRMC(tag string) func(*nex.Connection, *nex.RMCMessage) {
	return func(c *nex.Connection, req *nex.RMCMessage) {
		fmt.Printf("[SMO %s] pid=%d proto=%#x method=%d call=%d\n", tag, c.PID, req.Protocol, req.Method, req.CallID)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func requireSignedToken() bool {
	v := os.Getenv("NEXTENDO_REQUIRE_SIGNED_TOKEN")
	return v == "1" || v == "true"
}
