package registry

import (
	"crypto/sha256"
	"encoding/hex"
)

// It convert the key to it's sha256 hash and provides, encoded hash.
func hash(key []byte) string {
	hash := sha256.Sum256(key)
	return hex.EncodeToString(hash[:])
}
