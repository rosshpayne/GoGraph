package util

import (
	"testing"
	"time"
)

func TestDecodeb64(t *testing.T) {

	t0 := time.Now()
	//uidb64 := UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	uidb64 := MakeUIDb64()
	//	t.Logf("uidb64: %x %s %d\n", uidb64, uidb64, len(uidb64))
	u := uidb64.Decode()
	t1 := time.Now()
	t.Logf("%v\n", t1.Sub(t0))
	t.Logf("uidb64: %d %d %s %d\n", uidb64, len(uidb64), uidb64, len(uidb64.String()))
	t.Logf("decode: %d %d %s\n", u, len(u), u.String())
}

func TestMakeUID(t *testing.T) {

	t0 := time.Now()
	//uidb64 := UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	uid := MakeUID()
	t1 := time.Now()
	t.Logf("%v\n", t1.Sub(t0))
	t.Logf("uid: %d %d %s %d\n", uid, len(uid), uid, len(uid.String()))
}
