package main

import (
	"bytes"
	"testing"
)

func TestAchievementPayloadPreservesClientData(t *testing.T) {
	r := &balloonRecord{MetaBinary: []byte{1, 0, 255, 7}, CreatedAt: 12345}
	payload, created := achievementPayload(r)
	if !bytes.Equal(payload, r.MetaBinary) || created != r.CreatedAt {
		t.Fatal("client data changed")
	}
	payload[0] = 99
	if r.MetaBinary[0] != 1 {
		t.Fatal("payload aliases stored data")
	}
	for _, empty := range []*balloonRecord{nil, {CreatedAt: 123}} {
		payload, _ := achievementPayload(empty)
		if len(payload) != 0 {
			t.Fatal("fabricated achievement payload")
		}
	}
}
