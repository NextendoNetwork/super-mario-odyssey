package main

import (
	"fmt"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

// Balloon World reads its tuning from GetIntegerSettings; a missing key reads as 0.
var smoIntegerSettings = map[uint16]int32{
	0x0a: 10,  // minimum Play cost
	0x0b: 5,   // coin rounding step
	0x0c: 5,   // % added per star rank (PlRank)
	0x0d: 30,  // Play as % of Reward past the first tier
	0x0e: 100, // % of the card's Reward paid on a find
	0x0f: 30,  // Reward tier 1
	0x10: 40,  // Reward tier 2
	0x11: 50,  // Reward tier 3
	0x12: 60,  // Reward tier 4
	0x13: 80,  // Reward tier 5
	0x14: 1,   // tier 2 threshold
	0x15: 5,   // tier 3 threshold
	0x16: 20,  // tier 4 threshold
	0x17: 50,  // tier 5 threshold
	0x18: 1,   // success bonus per unit
	0x19: 10,  // success bonus % per step
	0x1a: 100, // success bonus % cap
	0x1c: 32,  // find-time level used when a balloon has none (40s)
	0x32: 30,  // Hide It seconds
	0x5a: 10,  // hider payout per failed attempt
	0x5b: 2,   // hider payout growth per attempt
}

func smoUtilityHandler() nex.RMCHandler {
	base := nex.UtilityHandler()
	return func(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		if req.Method != nex.MethodGetIntegerSettings {
			return base(conn, req)
		}
		index := nex.NewStreamIn(req.Body, conn.Settings).U32()
		fmt.Printf("[SMO Utility] pid=%d GetIntegerSettings index=%d -> %v\n", conn.PID, index, smoIntegerSettings)
		out := nex.NewStreamOut(conn.Settings)
		nex.WriteMap(out, smoIntegerSettings, func(o *nex.StreamOut, k uint16) { o.U16(k) }, func(o *nex.StreamOut, v int32) { o.S32(v) })
		return nex.NewRMCSuccess(conn.Settings, nex.ProtocolUtility, req.Method, req.CallID, out.Bytes())
	}
}
