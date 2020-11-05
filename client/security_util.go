package client

import (
	"crypto/sha1"
	"encoding/hex"
)

func Scramble411(data *[]byte,seed *[]byte) []byte {
	crypt := sha1.New()

	//stage1
	crypt.Write(*data)
	stage1 := crypt.Sum(nil)

	//stage2
	crypt.Reset()
	crypt.Write(stage1)
	stage2 := crypt.Sum(nil)

	//stage3
	crypt.Reset()
	crypt.Write(*seed)
	crypt.Write(stage2)
	stage3 := crypt.Sum(nil)
	for i := range stage3 {
		stage3[i] ^= stage1[i]
	}

	return stage3
}

func ByteSliceToHexString(data []byte) string {
	return hex.EncodeToString(data)
}
