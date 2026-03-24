package inventory

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

const (
	// DefaultRandomSympathNameLength is the default number of random
	// alphanumeric characters used in a generated .sympath filename.
	DefaultRandomSympathNameLength = 10

	randomSympathAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var randomSympathAlphabetSize = big.NewInt(int64(len(randomSympathAlphabet)))

// NewRandomSympathFilename returns a random alphanumeric .sympath
// filename using [DefaultRandomSympathNameLength] characters.
func NewRandomSympathFilename() (string, error) {
	return RandomSympathFilename(DefaultRandomSympathNameLength)
}

// RandomSympathFilename returns a random alphanumeric .sympath
// filename with exactly length characters before the extension.
//
// Length must be at least [DefaultRandomSympathNameLength] so default
// callers get a collision-resistant basename even when many files are
// copied into the same directory for later consolidation.
func RandomSympathFilename(length int) (string, error) {
	if length < DefaultRandomSympathNameLength {
		return "", fmt.Errorf("random .sympath filename length must be at least %d", DefaultRandomSympathNameLength)
	}

	name := make([]byte, length)
	for i := range name {
		n, err := rand.Int(rand.Reader, randomSympathAlphabetSize)
		if err != nil {
			return "", err
		}
		name[i] = randomSympathAlphabet[n.Int64()]
	}

	return string(name) + ".sympath", nil
}
