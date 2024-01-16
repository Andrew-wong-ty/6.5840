package raft

import (
	"fmt"
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
func min(a int, b int) int {
	m := a
	if b < a {
		m = b
	}
	return m
}

func randomElectionDuration() time.Duration {
	ms := 500 + (rand.Int63() % 200)
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

func generateEventId(rf *Raft) string {
	return generateRandomString(5) + "_T" + strconv.Itoa(rf.currentTerm)
}

func convertCommandToString(Command interface{}) string {
	commandStr := fmt.Sprintf("%v", Command)
	//if len(commandStr) > 10 {
	//	convertedCommand := fmt.Sprintf("%s...%s", commandStr[:5], commandStr[len(commandStr)-5:])
	//	return convertedCommand
	//}
	return fmt.Sprint(commandStr)
}
