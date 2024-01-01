package raft

import (
	"math/rand"
	"strconv"
	"time"
)

// *******helper functions******
const charset string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func max(a int, b int) int {
	m := a
	if b > a {
		m = b
	}
	return m
}

func randomElectionDuration() time.Duration {
	ms := 800 + (rand.Int63() % 200) // [800, 999] ms
	duration := time.Duration(ms) * time.Millisecond
	return duration
}
func generateRandomString(length int) string {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func generateEventId(length int, rf *Raft) string {
	return generateRandomString(5) + "_T" + strconv.Itoa(rf.currentTerm)
}
