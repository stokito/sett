package sett

import (
	"crypto/rand"
)

// Ref: https://elithrar.github.io/article/generating-secure-random-numbers-crypto-rand/

// GenerateID returns securely generated random bytes
// Returns error if the system does not have a random generator
func GenerateID(length int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}

	return string(bytes), nil
}
