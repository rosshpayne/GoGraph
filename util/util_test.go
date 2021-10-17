package util

import (
	"fmt"
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
	uid, _ := MakeUID()

	t1 := time.Now()

	uids := uid.String()
	fmt.Println("uids: ", uids)

	uidb64 := uid.Encodeb64()
	fmt.Println("uidb64: ", uidb64)

	uidtos := uid.ToString()
	fmt.Println("uidtos: ", uidtos)

	uid2 := UID(uids) // convert does NOT do a Decode() - uid2 is still a b64uid ie. 24 byte []byte rather than 24 byte string

	fmt.Println("len(uids ", len(uids))
	fmt.Println("len(uid2 ", len(uid2))
	fmt.Println("len(uid ", len(uid))

	fmt.Println("Uid2:", uid2)
	fmt.Println("Uid2:", uid2.String())
	fmt.Println("Uid2:", uid2.Encodeb64())
	
	fmt.Println("== ub64uid conversion ===")
	fmt.Println("uids: ", uids)
	fmt.Println("len(u ", len(u))

	uid_ := UIDb64(uids).Decode()
	fmt.Println("len(uid_) uid_: ", len(uid_), uid_.String())

	uiddecode := uidb64.Decode()
	fmt.Println("len(uidb64 ", len(uidb64))
	fmt.Println("len(uiddecode ", len(uiddecode))
	fmt.Println("Decode:", uiddecode)

	//uid3 := uids
	fmt.Println("Uid2_:", uid2)
	t.Logf("%v\n", t1.Sub(t0))
	t.Logf("uid: %d %d %s %d\n", uid, len(uid), uid, len(uid.String()))
}
