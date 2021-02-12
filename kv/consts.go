// Package kv provides utilities to interact with
// a kv storage structure
package kv

const (
	keyInitialCharacter  = byte('=')
	metaInitialCharacter = byte('_')
)

var (
	keyInitialCharacterBytes = []byte{keyInitialCharacter}
)
