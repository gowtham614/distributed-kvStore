package raft

import (
	"log"
	"math"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func max(a int, b int) int {
	return int(math.Max(float64(a), float64(b)))
}
