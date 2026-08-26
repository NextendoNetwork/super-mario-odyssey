// datastore.go implements the DataStore protocol (0x73) for real. Unlike ARMS/SSBU, where
// DataStore is a peripheral stub the client barely touches, Odyssey's entire online feature
// (Balloon World: place a "capture" balloon, other players search for and pop it) IS
// DataStore -- there is no matchmaking/lobby protocol at all for this title. So this can't be
// a replay stub; it has to actually store and search real records.
//
// Wire shapes are from https://github.com/kinnay/NintendoClients/wiki/Data-Store-Protocol and
// .../Data-Store-Protocol-(SMO) -- not yet cross-checked against a real Odyssey capture, so
// field-level mistakes are possible. Every method logs proto+method+pid so a real capture can
// correct this quickly, same as ranking.go's "measured later" methods.
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

// protocolDataStore -- nextendo-nex defines no constant for this (see arms_stubs.go's own
// comment); every game that needs it declares its own copy.
const protocolDataStore uint16 = 0x73

// DataStore method IDs actually implemented below. The rest fall through to notImplemented,
// logged, so an observed-but-unhandled call is easy to spot in the journal.
const (
	methodDeleteObject       uint32 = 4
	methodGetMeta            uint32 = 8
	methodGetMetas           uint32 = 9
	methodSearchObject       uint32 = 12
	methodRateObject         uint32 = 15
	methodGetRating          uint32 = 16
	methodRateObjects        uint32 = 40
	methodPostMetaBinary     uint32 = 21
	methodPreparePostObject  uint32 = 24
	methodPrepareGetObject   uint32 = 25
	methodCompletePostObject uint32 = 26

	// SMO's own extension (Data-Store-Protocol-(SMO)).
	methodAddToBufferQueue  uint32 = 47
	methodAddToBufferQueues uint32 = 48
	methodGetBufferQueue    uint32 = 49
	methodGetBufferQueues   uint32 = 50
	methodClearBufferQueues uint32 = 51
	methodSearchBalloon     uint32 = 52
	methodFetchMyInfos      uint32 = 53
)

// resultDataStoreNotFound is DataStore::NotFound (facility 105) -- same code ARMS/SSBU already
// use for "no such object", per Pretendo's documented result-code table.
const resultDataStoreNotFound uint32 = 0x80690004

// --- storage -----------------------------------------------------------------------------
//
// Real Nintendo backs DataStore objects with S3: PreparePostObject/PrepareGetObject hand the
// console a signed upload/download URL, and the actual bytes never touch the NEX server. We
// don't have S3, so objectstore.go stands in as a tiny local HTTPS blob host on its own port,
// and the URLs we hand back point at ourselves instead of Amazon.

type balloonRecord struct {
	DataID      uint64
	OwnerID     uint64
	Size        uint32
	Name        string
	DataType    uint16
	MetaBinary  []byte
	Tags        []string
	CreatedAt   uint64 // DateTime, wire format -- see CreatedWall for a usable Go time
	UpdatedAt   uint64
	CreatedWall time.Time
	Complete    bool // CompletePostObject seen -- excluded from search/get until then
	Buffers     map[int8][][]byte
	RatingSum   int64
	RatingN     uint32
}

var (
	dsMu      sync.Mutex
	dsRecords = map[uint64]*balloonRecord{}
	dsNextID  atomic.Uint64 // dataId counter, seeded below

	dsFile  = envOr("SMO_DATASTORE_FILE", "smo_datastore.json")
	dsDirty atomic.Bool
)

func init() {
	dsNextID.Store(1)
}

// persistedRecord is balloonRecord minus the in-memory-only Buffers map (kept simple: buffer
// queues -- the per-slot capture-pose screenshots -- are re-uploaded by the client each session
// far more often than they're read back across a restart, so losing them on restart is a much
// smaller loss than losing balloon placements themselves).
type persistedRecord struct {
	DataID, OwnerID      uint64
	Size                 uint32
	Name                 string
	DataType             uint16
	MetaBinary           []byte
	Tags                 []string
	CreatedAt, UpdatedAt uint64
	CreatedWallUnix      int64
	Complete             bool
	RatingSum            int64
	RatingN              uint32
}

func dsLoad() {
	b, err := os.ReadFile(dsFile)
	if err != nil {
		return
	}
	var recs []persistedRecord
	if json.Unmarshal(b, &recs) != nil {
		return
	}
	dsMu.Lock()
	defer dsMu.Unlock()
	for _, r := range recs {
		wall := time.Unix(r.CreatedWallUnix, 0)
		if r.CreatedWallUnix == 0 {
			wall = time.Now()
		}
		dsRecords[r.DataID] = &balloonRecord{
			DataID: r.DataID, OwnerID: r.OwnerID, Size: r.Size, Name: r.Name,
			DataType: r.DataType, MetaBinary: r.MetaBinary, Tags: r.Tags,
			CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, CreatedWall: wall, Complete: r.Complete,
			Buffers: map[int8][][]byte{}, RatingSum: r.RatingSum, RatingN: r.RatingN,
		}
		if r.DataID >= dsNextID.Load() {
			dsNextID.Store(r.DataID + 1)
		}
	}
	fmt.Printf("[SMO DataStore] loaded %d record(s) from %s\n", len(recs), dsFile)
}

func dsFlusher() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if !dsDirty.CompareAndSwap(true, false) {
				continue
			}
			dsMu.Lock()
			recs := make([]persistedRecord, 0, len(dsRecords))
			for _, r := range dsRecords {
				recs = append(recs, persistedRecord{
					DataID: r.DataID, OwnerID: r.OwnerID, Size: r.Size, Name: r.Name,
					DataType: r.DataType, MetaBinary: r.MetaBinary, Tags: r.Tags,
					CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, CreatedWallUnix: r.CreatedWall.Unix(),
					Complete: r.Complete, RatingSum: r.RatingSum, RatingN: r.RatingN,
				})
			}
			dsMu.Unlock()
			if b, err := json.Marshal(recs); err == nil {
				_ = os.WriteFile(dsFile, b, 0o644)
			}
		}
	}()
}

func dsMarkDirty() { dsDirty.Store(true) }

// --- protocol handler ----------------------------------------------------------------------

func DataStoreHandler() nex.RMCHandler {
	return func(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		fmt.Printf("[SMO DataStore] pid=%d method=%d call=%d bodyLen=%d\n",
			conn.PID, req.Method, req.CallID, len(req.Body))
		switch req.Method {
		case methodGetMeta:
			return dsGetMeta(conn, req)
		case methodGetMetas:
			return dsGetMetas(conn, req)
		case methodPreparePostObject:
			return dsPreparePostObject(conn, req)
		case methodCompletePostObject:
			return dsCompletePostObject(conn, req)
		case methodPrepareGetObject:
			return dsPrepareGetObject(conn, req)
		case methodDeleteObject:
			return dsDeleteObject(conn, req)
		case methodSearchObject:
			return dsSearchObject(conn, req)
		case methodRateObject:
			return dsRateObject(conn, req)
		case methodGetRating:
			return dsGetRating(conn, req)
		case methodRateObjects:
			return dsRateObjects(conn, req)
		case methodPostMetaBinary:
			return dsPostMetaBinary(conn, req)
		case methodAddToBufferQueue:
			return dsAddToBufferQueue(conn, req)
		case methodAddToBufferQueues:
			return dsAddToBufferQueues(conn, req)
		case methodGetBufferQueue:
			return dsGetBufferQueue(conn, req)
		case methodGetBufferQueues:
			return dsGetBufferQueues(conn, req)
		case methodClearBufferQueues:
			return dsClearBufferQueues(conn, req)
		case methodSearchBalloon:
			return dsSearchBalloon(conn, req)
		case methodFetchMyInfos:
			return dsFetchMyInfos(conn, req)
		default:
			return notImplementedDS(conn, req)
		}
	}
}

func notImplementedDS(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	fmt.Printf("[SMO DataStore] UNHANDLED method=%d pid=%d bodyLen=%d body=%x\n",
		req.Method, conn.PID, len(req.Body), req.Body)
	return nex.NewRMCError(conn.Settings, protocolDataStore, req.CallID, nex.ResultCoreNotImplemented)
}

// readPermission consumes a DataStorePermission (Uint8 permission, List<PID> recipientIds) --
// we don't enforce it, just need to advance the stream cursor correctly for whatever follows.
func readPermission(in *nex.StreamIn) {
	in.U8()
	nex.ReadList(in, func(in *nex.StreamIn) uint64 { return in.PID() })
}

func writePermission(out *nex.StreamOut) {
	out.U8(0) // 0 = public, matches "anyone can find this balloon"
	nex.WriteList(out, []uint64{}, func(o *nex.StreamOut, v uint64) { o.PID(v) })
}

// readStructHeader consumes a flat (non-inherited) Structure's [u8 version][u32 length] wire
// header -- present whenever Settings.StructHeader is on (always true for Switch/NEX 4.x,
// see nextendo-nex's NewSwitchSettings) -- and returns a substream scoped to just that
// structure's body. Every DataStore request parameter the wiki marks [Structure] (almost all
// of them -- DataStoreGetMetaParam, DataStorePreparePostParam, DataStoreSearchBalloonParam,
// DataStoreFetchMyInfosParam, ...) needs this before its fields can be read, or every field
// after it desyncs. Confirmed live: a real FetchMyInfos call's 45-byte body only parses
// cleanly (17 balloonDataTypes entries, a plausible real count) once this header is
// accounted for -- skipping it was the actual cause of the client's 2306-0111 after this
// call, not a field-level mistake in what followed.
func readStructHeader(in *nex.StreamIn) *nex.StreamIn {
	if !in.Settings.StructHeader {
		return in
	}
	in.U8() // version -- tolerated, same as ReadStructure elsewhere
	return in.Substream()
}

// writeStructHeader wraps body's output in a flat Structure's [u8 version][u32 length] wire
// header, mirroring readStructHeader for responses whose type (or an element of a List) the
// wiki marks [Structure] (DataStoreReqPostInfo, DataStoreReqGetInfo, DataStoreSearchResult,
// DataStoreSearchBalloonResult(Set), DataStoreFetchMyInfos*, ...).
func writeStructHeader(out *nex.StreamOut, body func(*nex.StreamOut)) {
	if !out.Settings.StructHeader {
		body(out)
		return
	}
	sub := nex.NewStreamOut(out.Settings)
	body(sub)
	out.U8(0)
	out.Buffer(sub.Bytes())
}

// --- GetMeta / GetMetas ----------------------------------------------------------------------

// includeMetaBinary defaults true for every caller except dsGetMeta, which -- per the wiki's
// documented DataStoreGetMetaParam.resultOption bitmask (0x4 = include metaBinary) -- must omit
// the field entirely (not send a zero-length one; the field itself isn't on the wire) unless the
// caller actually asked for it. Real bug found via live testing 2026-08-23: a real Balloon Code
// lookup for the player's own balloon (GetMeta, bodyLen=37) came back 2306-0116
// (Core::BufferOverflow) -- this response unconditionally included metaBinary regardless of
// resultOption, desyncing every field after it whenever the request didn't ask for it.
func metaInfoLevels(r *balloonRecord, includeMetaBinary bool) []nex.Level {
	return []nex.Level{{Save: func(out *nex.StreamOut) {
		out.U64(r.DataID)
		out.PID(r.OwnerID)
		out.U32(r.Size)
		out.String(r.Name)
		out.U16(r.DataType)
		if includeMetaBinary {
			out.QBuffer(r.MetaBinary)
		}
		writePermission(out) // permission
		writePermission(out) // delPermission
		out.DateTime(r.CreatedAt)
		out.DateTime(r.UpdatedAt)
		out.U16(0)      // period: never expires
		out.U8(0)       // status
		out.U32(0)      // referredCnt
		out.U32(0)      // referDataId
		out.U32(0)      // flag
		out.DateTime(0) // referredTime
		out.DateTime(0) // expireTime
		nex.WriteList(out, r.Tags, func(o *nex.StreamOut, t string) { o.String(t) })
		nex.WriteList(out, []int8{}, func(o *nex.StreamOut, v int8) {}) // ratings: none tracked per-slot
	}}}
}

type metaInfoStruct struct {
	r                 *balloonRecord
	includeMetaBinary bool
}

func (m *metaInfoStruct) Levels() []nex.Level {
	return metaInfoLevels(m.r, m.includeMetaBinary)
}

// resultOptionMetaBinary is DataStoreGetMetaParam.resultOption's documented 0x4 bit: "include
// metaBinary in the response". See metaInfoLevels' comment for why honoring this matters.
const resultOptionMetaBinary uint8 = 0x4

func dsGetMeta(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s))
	dataID := in.U64()
	// persistenceTarget is itself a [Structure] nested inside this one -- needs its own header.
	_ = readStructHeader(in) // ownerId(PID) + persistenceSlotId(u16); not used to answer
	resultOption := in.U8()
	// accessPassword follows but isn't needed to answer.
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	r, ok := dsRecords[dataID]
	var knownIDs []uint64
	for id := range dsRecords {
		knownIDs = append(knownIDs, id)
	}
	dsMu.Unlock()
	fmt.Printf("[SMO Balloon] GetMeta pid=%d requested dataId=%d resultOption=%#x found=%v knownIds=%v\n",
		conn.PID, dataID, resultOption, ok, knownIDs)
	if !ok {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, resultDataStoreNotFound)
	}
	out := nex.NewStreamOut(s)
	out.Add(&metaInfoStruct{r: r, includeMetaBinary: resultOption&resultOptionMetaBinary != 0})
	fmt.Printf("[SMO Balloon] GetMeta pid=%d responding with dataId=%d ownerId=%d type=%d metaLen=%d includeMetaBinary=%v respBytes=%x\n",
		conn.PID, r.DataID, r.OwnerID, r.DataType, len(r.MetaBinary), resultOption&resultOptionMetaBinary != 0, out.Bytes())
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

func dsGetMetas(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := nex.NewStreamIn(req.Body, s)
	ids := nex.ReadList(in, func(in *nex.StreamIn) uint64 {
		// Each element is its own DataStoreGetMetaParam ([Structure]) -- same per-element
		// header WriteList's callers already account for on the write side (see
		// dsGetMetas' own response below, and ranking.go's "each element still needs its
		// OWN struct-header wrapping" precedent).
		p := readStructHeader(in)
		return p.U64()
	})
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	metas := make([]*metaInfoStruct, 0, len(ids))
	results := make([]uint32, 0, len(ids))
	for _, id := range ids {
		if r, ok := dsRecords[id]; ok {
			metas = append(metas, &metaInfoStruct{r: r, includeMetaBinary: true})
			results = append(results, nex.SuccessResult(0).Code)
		} else {
			metas = append(metas, &metaInfoStruct{r: &balloonRecord{}, includeMetaBinary: true})
			results = append(results, nex.ErrorResult(resultDataStoreNotFound).Code)
		}
	}
	dsMu.Unlock()
	out := nex.NewStreamOut(s)
	nex.WriteList(out, metas, func(o *nex.StreamOut, m *metaInfoStruct) { o.Add(m) })
	nex.WriteList(out, results, func(o *nex.StreamOut, v uint32) { o.U32(v) })
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// --- Post / Get object -----------------------------------------------------------------------

func dsPreparePostObject(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s))
	size := in.U32()
	name := in.String()
	dataType := in.U16()
	metaBinary := in.QBuffer()
	readPermission(in) // permission
	readPermission(in) // delPermission
	in.U32()           // flag
	in.U16()           // period
	in.U32()           // referDataId
	tags := nex.ReadList(in, func(in *nex.StreamIn) string { return in.String() })
	// ratingInitParams / persistenceInitParam / extraData follow -- not needed to answer.
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}

	dataID := dsNextID.Add(1) - 1
	now := nex.NowDateTime().Value()
	rec := &balloonRecord{
		DataID: dataID, OwnerID: conn.PID, Size: size, Name: name, DataType: dataType,
		MetaBinary: metaBinary, Tags: tags, CreatedAt: now, UpdatedAt: now, CreatedWall: time.Now(),
		Buffers: map[int8][][]byte{},
	}
	dsMu.Lock()
	dsRecords[dataID] = rec
	dsMu.Unlock()
	dsMarkDirty()

	url, token := objectUploadURL(dataID)
	out := nex.NewStreamOut(s)
	// DataStoreReqPostInfo is itself a [Structure] -- the whole response body gets ONE
	// top-level struct-header wrapper, same as DataStoreReqGetInfo below.
	writeStructHeader(out, func(o *nex.StreamOut) {
		o.U64(dataID)
		o.String(url)
		nex.WriteList(o, []struct{ k, v string }{{"X-Object-Token", token}},
			func(o *nex.StreamOut, kv struct{ k, v string }) { o.String(kv.k); o.String(kv.v) })
		nex.WriteList(o, []struct{ k, v string }{}, func(o *nex.StreamOut, kv struct{ k, v string }) {}) // formFields
		o.Buffer(nil)                                                                                    // rootCaCert: none, our own self-signed cert is already trusted by the console via its normal chain
	})
	fmt.Printf("[SMO Balloon] pid=%d PreparePostObject -> dataId=%d name=%q type=%d size=%d\n",
		conn.PID, dataID, name, dataType, size)
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// dsPostMetaBinary answers PostMetaBinary(21) -- observed live as the FIRST real write call
// Odyssey makes (right after FetchMyInfos), ahead of PreparePostObject/24. It takes the same
// DataStorePreparePostParam ([Structure]) as PreparePostObject, but unlike that method there's
// no separate upload URL / CompletePostObject step: the meta binary IS the whole payload, so
// the record is stored complete immediately. Response is just the new dataId (a plain Uint64,
// not itself a [Structure], so no header on the way out).
func dsPostMetaBinary(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s))
	size := in.U32()
	name := in.String()
	dataType := in.U16()
	metaBinary := in.QBuffer()
	readPermission(in) // permission
	readPermission(in) // delPermission
	in.U32()           // flag
	in.U16()           // period
	in.U32()           // referDataId
	tags := nex.ReadList(in, func(in *nex.StreamIn) string { return in.String() })
	// ratingInitParams / persistenceInitParam / extraData follow -- not needed to answer.
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}

	dataID := dsNextID.Add(1) - 1
	now := nex.NowDateTime().Value()
	rec := &balloonRecord{
		DataID: dataID, OwnerID: conn.PID, Size: size, Name: name, DataType: dataType,
		MetaBinary: metaBinary, Tags: tags, CreatedAt: now, UpdatedAt: now, CreatedWall: time.Now(),
		Complete: true, // no separate CompletePostObject call for this method
		Buffers:  map[int8][][]byte{},
	}
	dsMu.Lock()
	dsRecords[dataID] = rec
	dsMu.Unlock()
	dsMarkDirty()
	replaced := dsDeletePreviousBalloon(conn.PID, dataType, dataID)

	out := nex.NewStreamOut(s)
	out.U64(dataID)
	fmt.Printf("[SMO Balloon] pid=%d PostMetaBinary -> dataId=%d name=%q type=%d size=%d metaLen=%d (replaced dataId=%d)\n",
		conn.PID, dataID, name, dataType, size, len(metaBinary), replaced)
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// dsDeletePreviousBalloon enforces Odyssey's real "one active hidden balloon at a time" rule --
// real bug found via live testing 2026-08-23: placing a second balloon left the first one still
// active in the pool as a separate record instead of replacing it (confirmed: dataIds 449 AND
// 450 both sitting complete simultaneously for the same player). Only balloons of the SAME
// dataType are replaced -- smoAchievementDataType(200) records are a different "kind" entirely
// and must never be touched by this. Returns the replaced dataId, or 0 if there was none.
func dsDeletePreviousBalloon(ownerID uint64, dataType uint16, keepID uint64) uint64 {
	dsMu.Lock()
	var old uint64
	for id, r := range dsRecords {
		if id != keepID && r.OwnerID == ownerID && r.DataType == dataType && r.Complete {
			old = id
			delete(dsRecords, id)
			break // Odyssey only ever has one active balloon per player; stop at the first match
		}
	}
	dsMu.Unlock()
	if old != 0 {
		dsMarkDirty()
		objectDelete(old)
	}
	return old
}

func dsCompletePostObject(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s))
	dataID := in.U64()
	isSuccess := in.Bool()
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	r, ok := dsRecords[dataID]
	if ok && isSuccess {
		r.Complete = true
	} else if ok && !isSuccess {
		delete(dsRecords, dataID)
	}
	dsMu.Unlock()
	dsMarkDirty()
	fmt.Printf("[SMO Balloon] pid=%d CompletePostObject dataId=%d success=%v\n", conn.PID, dataID, isSuccess)
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, nil)
}

func dsPrepareGetObject(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s))
	dataID := in.U64()
	// lockId / persistenceTarget / accessPassword / extraData follow -- unused here.
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	r, ok := dsRecords[dataID]
	dsMu.Unlock()
	if !ok || !r.Complete {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, resultDataStoreNotFound)
	}
	url, token := objectDownloadURLFor(dataID, conn.PID)
	out := nex.NewStreamOut(s)
	// DataStoreReqGetInfo is a [Structure] -- one top-level wrapper for the whole response.
	writeStructHeader(out, func(o *nex.StreamOut) {
		o.String(url)
		nex.WriteList(o, []struct{ k, v string }{{"X-Object-Token", token}},
			func(o *nex.StreamOut, kv struct{ k, v string }) { o.String(kv.k); o.String(kv.v) })
		o.U32(r.Size)
		o.Buffer(nil) // rootCaCert
		o.U64(dataID)
	})
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

func dsDeleteObject(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s))
	dataID := in.U64()
	in.U64() // updatePassword: not enforced
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	delete(dsRecords, dataID)
	dsMu.Unlock()
	dsMarkDirty()
	objectDelete(dataID)
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, nil)
}

// --- Search (generic; SearchBalloon below is the one the Balloon World UI actually uses) -----

func dsSearchObject(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s))
	in.U8()                                                             // searchTarget
	nex.ReadList(in, func(in *nex.StreamIn) uint64 { return in.PID() }) // ownerIds
	in.U8()                                                             // ownerType
	nex.ReadList(in, func(in *nex.StreamIn) uint64 { return in.U64() }) // destinationIds
	dataType := in.U16()
	// remaining fields (date ranges, tags, ordering, resultRange, ...) aren't decoded --
	// same "don't risk desync on unconfirmed layout" call ranking.go makes for GetRanking.
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}

	dsMu.Lock()
	var matches []*balloonRecord
	for _, r := range dsRecords {
		if r.Complete && (dataType == 0 || r.DataType == dataType) {
			matches = append(matches, r)
		}
	}
	dsMu.Unlock()
	sort.Slice(matches, func(i, j int) bool { return matches[i].CreatedAt > matches[j].CreatedAt })
	if len(matches) > 100 {
		matches = matches[:100]
	}

	out := nex.NewStreamOut(s)
	// DataStoreSearchResult is a [Structure] -- one top-level wrapper for the whole response.
	writeStructHeader(out, func(o *nex.StreamOut) {
		o.U32(uint32(len(matches)))
		nex.WriteList(o, matches, func(o *nex.StreamOut, r *balloonRecord) {
			o.Add(&metaInfoStruct{r: r, includeMetaBinary: true})
		})
		o.U8(1) // totalCountType: exact
	})
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// --- Ratings (Balloon World's "nice throw" claps) --------------------------------------------

func dsRateObject(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	// NOTE: unlike every other method in this file, RateObject/GetRating's exact request
	// param type wasn't confirmed as [Structure] from the wiki page fetched for this build
	// (only the method ID table was available, not the per-method request/response detail) --
	// left unwrapped rather than guessing a header that might not be there. If this method
	// ever gets exercised by a real client and fails the same way FetchMyInfos initially did,
	// check this first.
	s := conn.Settings
	in := nex.NewStreamIn(req.Body, s)
	dataID := in.U64()
	in.S8()           // slot
	value := in.S64() // ratingValue
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	if r, ok := dsRecords[dataID]; ok {
		r.RatingSum += value
		r.RatingN++
	}
	dsMu.Unlock()
	dsMarkDirty()
	out := nex.NewStreamOut(s)
	out.S64(value)
	out.U32(1)
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

func dsGetRating(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	// See dsRateObject's note above -- same unconfirmed-structure-wrapping caveat.
	s := conn.Settings
	in := nex.NewStreamIn(req.Body, s)
	dataID := in.U64()
	in.S8() // slot
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	r, ok := dsRecords[dataID]
	dsMu.Unlock()
	if !ok {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, resultDataStoreNotFound)
	}
	out := nex.NewStreamOut(s)
	out.S64(r.RatingSum)
	out.U32(r.RatingN)
	out.S64(0) // initialValue
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// dsRateObjects answers RateObjects(40) -- wire format fully verified live 2026-08-23 against a
// real Hide It completion call (bodyLen=72, decoded byte-for-byte with zero leftover):
// List<DataStoreRatingTarget{dataId u64, slot s8}> targets, List<DataStoreRateObjectParam{
// ratingValue s32, accessPassword u64}> params (both [Structure]-wrapped per element),
// Bool transactional, Bool fetchRatings.
//
// The real captured call rated TWO targets in one shot: the just-placed balloon (dataId of an
// existing record, ratingValue=0) and a second, much smaller dataId (60) that isn't a balloon
// at all -- almost certainly a fixed/reserved system dataId Odyssey uses as a generic
// "increment a personal stat counter" slot (ratingValue=1, i.e. "+1 hidden count"), not
// something this server tracks as a real DataStore object. Unknown targets get NotFound in
// their result slot without failing the whole call.
func dsRateObjects(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := nex.NewStreamIn(req.Body, s)
	type target struct {
		dataID uint64
		slot   int8
	}
	targets := nex.ReadList(in, func(in *nex.StreamIn) target {
		p := readStructHeader(in)
		return target{dataID: p.U64(), slot: p.S8()}
	})
	type param struct {
		ratingValue int32
		accessPw    uint64
	}
	params := nex.ReadList(in, func(in *nex.StreamIn) param {
		p := readStructHeader(in)
		return param{ratingValue: p.S32(), accessPw: p.U64()}
	})
	in.Bool() // transactional -- not enforced, each target is applied independently
	fetchRatings := in.Bool()
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}

	n := len(targets)
	if len(params) < n {
		n = len(params)
	}
	ratings := make([]*balloonRecord, n)
	results := make([]uint32, n)
	dsMu.Lock()
	for i := 0; i < n; i++ {
		r, ok := dsRecords[targets[i].dataID]
		if !ok {
			results[i] = nex.ErrorResult(resultDataStoreNotFound).Code
			continue
		}
		r.RatingSum += int64(params[i].ratingValue)
		r.RatingN++
		ratings[i] = r
		results[i] = nex.SuccessResult(0).Code
	}
	dsMu.Unlock()
	dsMarkDirty()

	out := nex.NewStreamOut(s)
	nex.WriteList(out, ratings, func(o *nex.StreamOut, r *balloonRecord) {
		writeStructHeader(o, func(o *nex.StreamOut) {
			if r != nil && fetchRatings {
				o.S64(r.RatingSum)
				o.U32(r.RatingN)
			} else {
				o.S64(0)
				o.U32(0)
			}
			o.S64(0) // initialValue
		})
	})
	nex.WriteList(out, results, func(o *nex.StreamOut, v uint32) { o.U32(v) })
	fmt.Printf("[SMO Balloon] pid=%d RateObjects %d target(s)\n", conn.PID, n)
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// --- SMO extension: buffer queues -------------------------------------------------------------
//
// Kingdom capture screenshots (the pose photo baked into a placed balloon) ride these instead
// of the main object body -- AddToBufferQueue appends one more qBuffer onto dataId's slot,
// GetBufferQueue reads all of them back in order.

func dsAddToBufferQueue(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	// BufferQueueParam ([Structure]: dataId Uint64, slot Uint32) gets its own struct-header
	// wrapper; the trailing qBuffer is a plain (non-Structure) type and stays unwrapped.
	s := conn.Settings
	outer := nex.NewStreamIn(req.Body, s)
	in := readStructHeader(outer)
	dataID := in.U64()
	slot := int8(in.U32())
	buf := outer.QBuffer()
	if in.Err() != nil || outer.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	if r, ok := dsRecords[dataID]; ok {
		if r.Buffers == nil {
			r.Buffers = map[int8][][]byte{}
		}
		r.Buffers[slot] = append(r.Buffers[slot], buf)
	}
	dsMu.Unlock()
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, nil)
}

func dsAddToBufferQueues(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	// List<BufferQueueParam> params, List<qBuffer> buffers -- paired by index.
	s := conn.Settings
	in := nex.NewStreamIn(req.Body, s)
	type param struct {
		dataID uint64
		slot   uint32
	}
	params := nex.ReadList(in, func(in *nex.StreamIn) param {
		p := readStructHeader(in) // each element is its own BufferQueueParam ([Structure])
		return param{dataID: p.U64(), slot: p.U32()}
	})
	buffers := nex.ReadList(in, func(in *nex.StreamIn) []byte { return in.QBuffer() })
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	results := make([]uint32, len(params))
	dsMu.Lock()
	for i, p := range params {
		if r, ok := dsRecords[p.dataID]; ok && i < len(buffers) {
			if r.Buffers == nil {
				r.Buffers = map[int8][][]byte{}
			}
			r.Buffers[int8(p.slot)] = append(r.Buffers[int8(p.slot)], buffers[i])
			results[i] = nex.SuccessResult(0).Code
		} else {
			results[i] = nex.ErrorResult(resultDataStoreNotFound).Code
		}
	}
	dsMu.Unlock()
	out := nex.NewStreamOut(s)
	nex.WriteList(out, results, func(o *nex.StreamOut, v uint32) { o.U32(v) })
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

func dsGetBufferQueue(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s))
	dataID := in.U64()
	slot := int8(in.U32())
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	var bufs [][]byte
	if r, ok := dsRecords[dataID]; ok {
		bufs = r.Buffers[slot]
	}
	dsMu.Unlock()
	out := nex.NewStreamOut(s)
	nex.WriteList(out, bufs, func(o *nex.StreamOut, b []byte) { o.QBuffer(b) })
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

func dsGetBufferQueues(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := nex.NewStreamIn(req.Body, s)
	type param struct {
		dataID uint64
		slot   uint32
	}
	params := nex.ReadList(in, func(in *nex.StreamIn) param {
		p := readStructHeader(in) // each element is its own BufferQueueParam ([Structure])
		return param{dataID: p.U64(), slot: p.U32()}
	})
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	lists := make([][][]byte, len(params))
	results := make([]uint32, len(params))
	for i, p := range params {
		if r, ok := dsRecords[p.dataID]; ok {
			lists[i] = r.Buffers[int8(p.slot)]
			results[i] = nex.SuccessResult(0).Code
		} else {
			results[i] = nex.ErrorResult(resultDataStoreNotFound).Code
		}
	}
	dsMu.Unlock()
	out := nex.NewStreamOut(s)
	nex.WriteList(out, lists, func(o *nex.StreamOut, bufs [][]byte) {
		nex.WriteList(o, bufs, func(o *nex.StreamOut, b []byte) { o.QBuffer(b) })
	})
	nex.WriteList(out, results, func(o *nex.StreamOut, v uint32) { o.U32(v) })
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

func dsClearBufferQueues(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := nex.NewStreamIn(req.Body, s)
	type param struct {
		dataID uint64
		slot   uint32
	}
	params := nex.ReadList(in, func(in *nex.StreamIn) param {
		p := readStructHeader(in) // each element is its own BufferQueueParam ([Structure])
		return param{dataID: p.U64(), slot: p.U32()}
	})
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	dsMu.Lock()
	results := make([]uint32, len(params))
	for i, p := range params {
		if r, ok := dsRecords[p.dataID]; ok {
			delete(r.Buffers, int8(p.slot))
			results[i] = nex.SuccessResult(0).Code
		} else {
			results[i] = nex.ErrorResult(resultDataStoreNotFound).Code
		}
	}
	dsMu.Unlock()
	out := nex.NewStreamOut(s)
	nex.WriteList(out, results, func(o *nex.StreamOut, v uint32) { o.U32(v) })
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// --- SMO extension: the actual Balloon World hunt ---------------------------------------------

// balloonFoundMu/balloonFound tracks which balloons a PID has already popped, so SearchBalloon
// doesn't keep handing back ones they've already found this session. In-memory only: losing
// this on restart just means everyone's balloons look "fresh" again, which is harmless.
var (
	balloonFoundMu sync.Mutex
	balloonFound   = map[uint64]map[uint64]bool{} // pid -> dataId -> true
)

func dsSearchBalloon(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	// TEMP DIAG: bodyLen has consistently been 10, not the 9 my assumed layout (5-byte struct
	// header + u16 dataType + u8 userRank + u8 resultSetCount) predicts -- dump raw bytes to
	// find the real layout instead of continuing to guess.
	fmt.Printf("[SMO Balloon] SearchBalloon RAW body=%x\n", req.Body)
	in := readStructHeader(nex.NewStreamIn(req.Body, s)) // DataStoreSearchBalloonParam ([Structure])
	dataType := in.U16()
	in.U8() // userRank
	resultSetCount := in.U8()
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	if resultSetCount == 0 {
		resultSetCount = 1
	}

	balloonFoundMu.Lock()
	found := balloonFound[conn.PID]
	balloonFoundMu.Unlock()

	dsMu.Lock()
	var pool []*balloonRecord
	for _, r := range dsRecords {
		if !r.Complete || r.OwnerID == conn.PID || r.DataType == smoAchievementDataType {
			continue
		}
		if dataType != 0 && r.DataType != dataType {
			continue
		}
		if found[r.DataID] {
			continue
		}
		pool = append(pool, r)
	}
	dsMu.Unlock()
	sort.Slice(pool, func(i, j int) bool { return pool[i].CreatedAt > pool[j].CreatedAt })

	out := nex.NewStreamOut(s)
	// DataStoreSearchBalloonResultSet is itself a [Structure] wrapping the inner list, and
	// EACH DataStoreSearchBalloonResult inside it is ALSO its own [Structure] -- three
	// nesting levels of struct-header framing in total (outer List, then a header per
	// result-set, then a header per result), same "every level gets its own header" rule
	// ranking.go's rankingResult/rankRankData pair documents.
	nex.WriteList(out, splitIntoSets(pool, int(resultSetCount)), func(o *nex.StreamOut, set []*balloonRecord) {
		writeStructHeader(o, func(o *nex.StreamOut) {
			nex.WriteList(o, set, func(o *nex.StreamOut, r *balloonRecord) {
				writeStructHeader(o, func(o *nex.StreamOut) {
					o.U64(r.DataID)
					o.PID(r.OwnerID)
					o.U32(r.Size)
					o.String(r.Name)
					o.U16(r.DataType)
					o.QBuffer(r.MetaBinary)
					o.DateTime(r.CreatedAt)
					o.DateTime(r.UpdatedAt)
					// ownerDataId: the generic wiki doesn't say what this is for; live-tested
					// 2026-08-23 with 0 here and the client showed "0 seconds" to find the
					// balloon (real location/distance rendered fine, only the time was wrong)
					// -- testing whether Odyssey repurposes this Uint64 as the challenge's
					// time limit in seconds. Real Balloon World "Find It" runs are commonly
					// ~60s; try 60 first.
					o.U64(60)
					// ownerName: real bug found via live testing 2026-08-23 -- this was hardcoded
					// empty on the (wrong) assumption the client resolves the display name itself
					// via friends/BAAS. It doesn't: an empty String here rendered as a fixed-width
					// block of corrupt placeholder glyphs (reported as "12 slashes" for a name).
					// Always send something real rather than empty.
					o.String(dispName(r.OwnerID))
					o.Bool(false) // isFriendBalloon: we don't cross-reference the friends list here
					nex.WriteMap(o, map[int8]int64{}, func(o *nex.StreamOut, k int8) { o.S8(k) },
						func(o *nex.StreamOut, v int64) { o.S64(v) }) // ratings: not exposed per-slot yet
					nex.WriteMap(o, map[int8]int64{}, func(o *nex.StreamOut, k int8) { o.S8(k) },
						func(o *nex.StreamOut, v int64) { o.S64(v) }) // ownerRatings
				})
			})
		})
	})
	fmt.Printf("[SMO Balloon] pid=%d SearchBalloon dataType=%d -> %d candidate(s)\n", conn.PID, dataType, len(pool))
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// splitIntoSets divides pool into n roughly-even DataStoreSearchBalloonResultSet buckets. Real
// Nintendo's exact bucketing rule (per-kingdom? per-rank bracket?) isn't captured; this just
// keeps the response shape correct while giving every match a chance to surface.
func splitIntoSets(pool []*balloonRecord, n int) [][]*balloonRecord {
	if n <= 0 {
		n = 1
	}
	// Real bug found via live testing 2026-08-23: never create more sets than there are real
	// candidates. An EMPTY result set renders as a broken "ghost" balloon client-side (a
	// far-off distance + a garbage/random time) -- the exact same fallback-placeholder
	// behavior already confirmed for a fully empty pool, just scoped per-page instead of
	// whole-response. Cap to len(pool); floor of 1 so a genuinely empty pool still gets one
	// envelope set.
	if len(pool) > 0 && n > len(pool) {
		n = len(pool)
	}
	sets := make([][]*balloonRecord, n)
	for i, r := range pool {
		sets[i%n] = append(sets[i%n], r)
	}
	return sets
}

// smoAchievementDataType (200 / 0xC8) is the dataType Odyssey uses for its own per-player
// profile/achievement sync record, posted via PostMetaBinary(21) with an EMPTY body
// (size=0, no metaBinary, no name) -- confirmed live, every real capture of this call looks
// identical regardless of actual play progress. It is NOT a placed balloon: excluded from the
// normal balloons list and from SearchBalloon's pool.
//
// Real bug found via live testing: FetchMyInfos used to always echo a hardcoded all-zero
// achievement placeholder no matter what had actually been posted. The client never saw its
// own PostMetaBinary reflected back, so it looped FetchMyInfos -> PostMetaBinary -> FetchMyInfos
// forever (observed: 7+ iterations/sec, dataId climbing every call) instead of ever reaching
// the actual Balloon World UI. Echoing the player's real achievement record here is what
// breaks the loop.
const smoAchievementDataType uint16 = 200

// buildAchievementMetaBinary synthesizes the stats blob content: the client has never once sent
// real bytes here, and a real captured FetchMyInfos response proved our own wire framing is
// byte-exact correct (dataID/dataType/QBuffer length/name all land exactly where our code puts
// them) -- so the corruption isn't an encoding bug, it's that the client expects the name at a
// different byte offset (or a different total size) than offset 0. DIAGNOSTIC BUILD: plants a
// distinct "@NNN" marker (UTF-16LE) every 16 bytes across the buffer instead of one name at
// offset 0 -- whichever marker renders legibly in-game tells us the real offset directly. Revert
// to a single name at the confirmed offset once known.
func buildAchievementMetaBinary(pid uint64) []byte {
	buf := make([]byte, 256)
	for slot := 0; slot < 256; slot += 16 {
		marker := fmt.Sprintf("@%03d", slot)
		for i, u := range utf16.Encode([]rune(marker)) {
			binary.LittleEndian.PutUint16(buf[slot+i*2:], u)
		}
	}
	return buf
}

func dsFetchMyInfos(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	s := conn.Settings
	in := readStructHeader(nex.NewStreamIn(req.Body, s)) // DataStoreFetchMyInfosParam ([Structure])
	balloonDataTypes := nex.ReadList(in, func(in *nex.StreamIn) uint16 { return in.U16() })
	in.U16() // additionalOperation
	if in.Err() != nil {
		return nex.NewRMCError(s, protocolDataStore, req.CallID, nex.ResultCoreInvalidArgument)
	}
	// Real bug found via live testing 2026-08-23: this used to ignore balloonDataTypes entirely
	// and return every balloon the player owns across ALL kingdoms in one combined list. Once a
	// player has balloons active in more than one kingdom (each kingdom uses its own dataType --
	// confirmed live: 13 vs 14 for two different kingdoms), the client -- which only expects the
	// single entry relevant to the CURRENT kingdom back -- rendered every dynamic field (name,
	// time, coins) as a garbled placeholder glyph trying to reconcile the extra/mismatched
	// entries. Filtering to only the requested type(s) fixes it.
	wantType := func(t uint16) bool {
		if len(balloonDataTypes) == 0 {
			return true // no filter requested -- keep old behavior
		}
		for _, dt := range balloonDataTypes {
			if dt == t {
				return true
			}
		}
		return false
	}

	balloonFoundMu.Lock()
	found := balloonFound[conn.PID]
	balloonFoundMu.Unlock()

	dsMu.Lock()
	var mine []*balloonRecord
	var achievement *balloonRecord
	for _, r := range dsRecords {
		if !r.Complete || r.OwnerID != conn.PID {
			continue
		}
		if r.DataType == smoAchievementDataType {
			if achievement == nil || r.CreatedAt > achievement.CreatedAt {
				achievement = r
			}
			continue
		}
		if !wantType(r.DataType) {
			continue
		}
		mine = append(mine, r)
	}
	dsMu.Unlock()
	sort.Slice(mine, func(i, j int) bool { return mine[i].CreatedAt > mine[j].CreatedAt })

	out := nex.NewStreamOut(s)
	// DataStoreFetchMyInfosResult (the whole response) is itself a [Structure], each
	// DataStoreFetchMyInfosBalloonResult element is ALSO its own [Structure], and the
	// trailing DataStoreFetchMyInfosAchievementResult field is a THIRD [Structure] nested
	// inside the outer one -- this is the exact call whose missing top-level wrapper caused
	// the client's 2306-0111 on the first real test, so all three levels matter here.
	writeStructHeader(out, func(o *nex.StreamOut) {
		nex.WriteList(o, mine, func(o *nex.StreamOut, r *balloonRecord) {
			writeStructHeader(o, func(o *nex.StreamOut) {
				o.U64(r.DataID)
				o.U16(r.DataType)
				o.QBuffer(r.MetaBinary)
				o.DateTime(r.CreatedAt)
				o.DateTime(r.UpdatedAt)
				o.Bool(found[r.DataID]) // isCleared: whether the placer's OWN balloon has been popped by someone
				nex.WriteMap(o, map[int8]int64{}, func(o *nex.StreamOut, k int8) { o.S8(k) },
					func(o *nex.StreamOut, v int64) { o.S64(v) }) // ratings
				nex.WriteMap(o, map[int8][][]byte{}, func(o *nex.StreamOut, k int8) { o.S8(k) },
					func(o *nex.StreamOut, v [][]byte) {
						nex.WriteList(o, v, func(o *nex.StreamOut, b []byte) { o.QBuffer(b) })
					}) // buffers
			})
		})
		// DataStoreFetchMyInfosAchievementResult: echo the player's real smoAchievementDataType
		// record if PostMetaBinary has posted one, so the client sees its own sync reflected
		// back instead of looping forever (see smoAchievementDataType's comment above).
		//
		// The DateTime field here is a "now" sync anchor the client uses to compute the Hide
		// It/Find It challenge timer, not the achievement record's real creation time -- echoing
		// achievement.CreatedAt (set once, when the record was first posted) goes stale the
		// moment real time has passed since then, reproducing the same "elapsed time clamps to
		// 0 seconds" bug the nil-achievement fallback below was already fixed for. Always "now".
		writeStructHeader(o, func(o *nex.StreamOut) {
			dataID, dataType := uint64(0), uint16(0)
			if achievement != nil {
				dataID, dataType = achievement.DataID, achievement.DataType
			}
			o.U64(dataID)
			o.U16(dataType)
			o.QBuffer(buildAchievementMetaBinary(conn.PID))
			o.DateTime(nex.NowDateTime().Value())
			nex.WriteMap(o, map[int8]int64{}, func(o *nex.StreamOut, k int8) { o.S8(k) },
				func(o *nex.StreamOut, v int64) { o.S64(v) })
			nex.WriteMap(o, map[int8][][]byte{}, func(o *nex.StreamOut, k int8) { o.S8(k) },
				func(o *nex.StreamOut, v [][]byte) {
					nex.WriteList(o, v, func(o *nex.StreamOut, b []byte) { o.QBuffer(b) })
				})
		})
	})
	return nex.NewRMCSuccess(s, protocolDataStore, req.Method, req.CallID, out.Bytes())
}

// markBalloonFound is called by objectstore.go when a client downloads a balloon it didn't
// place -- the closest signal we have to "this player popped it" without a dedicated method
// for it in the captured method list (Odyssey likely infers this from the download itself,
// same as how PrepareGetObject is the only client-visible step in the whole "find" flow).
func markBalloonFound(pid, dataID uint64) {
	balloonFoundMu.Lock()
	if balloonFound[pid] == nil {
		balloonFound[pid] = map[uint64]bool{}
	}
	balloonFound[pid][dataID] = true
	balloonFoundMu.Unlock()
}
