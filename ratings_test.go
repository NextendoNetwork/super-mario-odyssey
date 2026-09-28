package main

import (
	"testing"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

func TestReadRatingInitParams(t *testing.T) {
	s := nex.NewSwitchSettings("afef0ecf", 40000)
	out := nex.NewStreamOut(s)
	type slot struct {
		slot    int8
		initial int64
	}
	nex.WriteList(out, []slot{{2, 45}, {3, 60}}, func(o *nex.StreamOut, v slot) {
		writeStructHeader(o, func(o *nex.StreamOut) {
			o.S8(v.slot)
			writeStructHeader(o, func(o *nex.StreamOut) {
				o.U8(0)          // flag
				o.U8(0)          // internalFlag
				o.U8(0)          // lockType
				o.S64(v.initial) // initialValue
				o.S32(-1)        // rangeMin
				o.S32(-1)        // rangeMax
				o.S8(0)          // periodHour
				o.S16(0)         // periodDuration
			})
		})
	})
	out.U16(0) // trailing persistenceInitParam data must be left unread
	in := nex.NewStreamIn(out.Bytes(), s)
	got := readRatingInitParams(in)
	if len(got) != 2 || got[2] != (ratingInfo{Total: 45, Initial: 45}) || got[3].Initial != 60 {
		t.Fatalf("ratings = %v", got)
	}
	if in.Remaining() != 2 {
		t.Fatalf("consumed wrong length, %d bytes left", in.Remaining())
	}
	if m := ratingsOf(nil); m == nil || len(m) != 0 {
		t.Fatalf("ratingsOf(nil) = %v", m)
	}
}

func TestReadPersistenceSlot(t *testing.T) {
	s := nex.NewSwitchSettings("afef0ecf", 40000)
	for _, want := range []uint16{3, 0xFFFF} {
		out := nex.NewStreamOut(s)
		writeStructHeader(out, func(o *nex.StreamOut) { o.U16(want); o.U8(1) })
		got := readPersistenceSlot(nex.NewStreamIn(out.Bytes(), s))
		if want == 0xFFFF && got != nil || want != 0xFFFF && (got == nil || *got != want) {
			t.Fatalf("slot %d -> %v", want, got)
		}
	}
}

// DataStorePermission is a Structure: a reader must land exactly on the next field.
func TestPermissionRoundTrip(t *testing.T) {
	s := nex.NewSwitchSettings("afef0ecf", 40000)
	out := nex.NewStreamOut(s)
	writePermission(out)
	writePermission(out)
	out.U32(0xDEADBEEF)
	if len(out.Bytes()) != 2*(5+1+4)+4 {
		t.Fatalf("encoded %d bytes", len(out.Bytes()))
	}
	in := nex.NewStreamIn(out.Bytes(), s)
	readPermission(in)
	readPermission(in)
	if v := in.U32(); v != 0xDEADBEEF || in.Err() != nil {
		t.Fatalf("next field %x err %v", v, in.Err())
	}
}

// Each map value must be a DataStoreRatingInfo Structure: header + s64 + u32 + s64.
func TestWriteRatingsEncodesRatingInfo(t *testing.T) {
	s := nex.NewSwitchSettings("afef0ecf", 40000)
	out := nex.NewStreamOut(s)
	writeRatings(out, map[int8]ratingInfo{2: {Total: 7, Count: 3, Initial: 5}})
	in := nex.NewStreamIn(out.Bytes(), s)
	if n := in.U32(); n != 1 {
		t.Fatalf("count %d", n)
	}
	if k := in.S8(); k != 2 {
		t.Fatalf("slot %d", k)
	}
	v := readStructHeader(in)
	if total, count, initial := v.S64(), v.U32(), v.S64(); total != 7 || count != 3 || initial != 5 || v.Err() != nil {
		t.Fatalf("value %d %d %d %v", total, count, initial, v.Err())
	}
	if in.Remaining() != 0 {
		t.Fatalf("%d trailing bytes", in.Remaining())
	}
}
