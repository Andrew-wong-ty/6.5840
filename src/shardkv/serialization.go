package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
)

// encodeMap takes a map[int64]uint64 and returns a base64 encoded string.
func encodeMap(data map[int64]uint64) string {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		panic(fmt.Sprintf("gob encode error, data=%v", data))
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

// decodeMap takes a base64 string and decodes it back into a map[int64]uint64.
func decodeMap(encodedStr string) map[int64]uint64 {
	var data map[int64]uint64
	decodedBytes, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		panic("base64 decode error")
	}
	buf := bytes.NewBuffer(decodedBytes)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&data)
	if err != nil {
		panic("gob decode error")
	}
	return data
}

// encodeConfig takes a Config object by value and returns a base64 encoded string.
func encodeConfig(cfg shardctrler.Config) string {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cfg)
	if err != nil {
		panic(fmt.Sprintf("gob encode error, cfg=%v", cfg))
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

// decodeConfig takes a base64 string and decodes it back into a Config object.
func decodeConfig(encodedStr string) shardctrler.Config {
	var cfg shardctrler.Config
	// Decode the base64 string to bytes
	decodedBytes, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		panic("base64 decode error")
	}
	buf := bytes.NewBuffer(decodedBytes)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&cfg)
	if err != nil {
		panic("gob decode error")
	}
	return cfg
}

func encodeSlice(data []int) string {
	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		panic(fmt.Sprintf("gob encode slice error, data=%v", data))
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func decodeSlice(encodedStr string) []int {
	var data []int
	// Decode the base64 string to bytes
	decodedBytes, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		panic("base64 decode error")
	}
	buf := bytes.NewBuffer(decodedBytes)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&data)
	if err != nil {
		panic("gob decode error")
	}
	return data
}

func encodeDB(data [shardctrler.NShards]map[string]string) string {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		panic(fmt.Sprintf("gob encode DB error, db=%v", data))
	}
	// Use base64 encoding to convert bytes to a safe string for transport or storage
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func decodeDB(encodedStr string) [shardctrler.NShards]map[string]string {
	var data [shardctrler.NShards]map[string]string
	// Decode the base64 string to bytes
	decodedBytes, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		panic("base64 decode error")
	}
	buf := bytes.NewBuffer(decodedBytes)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&data)
	if err != nil {
		panic("gob decode error")
	}
	return data
}

//// TODO: delete after debug
//func dbgCheckDBNotNil(data [shardctrler.NShards]map[string]string) {
//	for _, mmap := range data {
//		if mmap == nil {
//			panic("unexpected: map[string]string is nil")
//		}
//	}
//}
