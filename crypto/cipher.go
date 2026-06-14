package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// KeySize is the required DEK length: AES-256.
const KeySize = 32

const (
	versionGCM byte = 0x01 // AES-256-GCM
	nonceSize       = 12   // GCM standard nonce
	headerSize      = 3    // magic(2) + version(1)
)

// magic prefixes every ciphertext blob. 0xB1 is a UTF-8 continuation byte, so a valid
// UTF-8 text plaintext can never begin with it — this lets Open transparently pass
// through legacy un-encrypted rows during a mixed-store migration with no risk of a
// magic collision against real conversation content.
var magic = [2]byte{0xB1, 0x15}

// Crypto errors. ErrCiphertext / ErrVersion indicate a corrupt or unknown blob;
// callers must surface these, never fall back to treating the bytes as plaintext.
var (
	ErrKeySize    = errors.New("crypto: DEK must be 32 bytes (AES-256)")
	ErrCiphertext = errors.New("crypto: malformed ciphertext")
	ErrVersion    = errors.New("crypto: unsupported ciphertext version")
)

// Cipher performs authenticated encryption with AES-256-GCM. The zero value is usable
// and stateless, so it is safe to share across goroutines.
type Cipher struct{}

// Seal encrypts plaintext with dek, binding aad (the user_id) into the authentication
// tag. Output layout: magic(2) || version(1) || nonce(12) || ciphertext+tag.
func (Cipher) Seal(dek, plaintext, aad []byte) ([]byte, error) {
	if len(dek) != KeySize {
		return nil, ErrKeySize
	}
	gcm, err := newGCM(dek)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: nonce: %w", err)
	}
	out := make([]byte, 0, headerSize+nonceSize+len(plaintext)+gcm.Overhead())
	out = append(out, magic[0], magic[1], versionGCM)
	out = append(out, nonce...)
	return gcm.Seal(out, nonce, plaintext, aad), nil
}

// Open reverses Seal. A blob WITHOUT the magic prefix is treated as legacy plaintext
// and returned unchanged with encrypted=false — this is the mixed-store read path.
// A blob WITH the prefix is authenticated against aad and decrypted; a tag mismatch
// (wrong key, wrong user_id, or tampering) returns an error.
func (Cipher) Open(dek, blob, aad []byte) (plaintext []byte, encrypted bool, err error) {
	if !IsEncrypted(blob) {
		return blob, false, nil // legacy plaintext passthrough
	}
	if blob[2] != versionGCM {
		return nil, false, ErrVersion
	}
	if len(dek) != KeySize {
		return nil, false, ErrKeySize
	}
	rest := blob[headerSize:]
	if len(rest) < nonceSize {
		return nil, false, ErrCiphertext
	}
	nonce, ct := rest[:nonceSize], rest[nonceSize:]
	gcm, err := newGCM(dek)
	if err != nil {
		return nil, false, err
	}
	pt, err := gcm.Open(nil, nonce, ct, aad)
	if err != nil {
		return nil, false, fmt.Errorf("crypto: gcm open: %w", err)
	}
	return pt, true, nil
}

// IsEncrypted reports whether blob carries the ciphertext magic prefix. Used by the
// read path to distinguish encrypted rows from legacy plaintext.
func IsEncrypted(blob []byte) bool {
	return len(blob) >= headerSize && blob[0] == magic[0] && blob[1] == magic[1]
}

func newGCM(dek []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
