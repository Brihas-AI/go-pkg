package crypto

import (
	"context"
	"encoding/base64"
	"errors"
)

// Encryptor errors.
var (
	// ErrNoUser is returned when an encrypt/decrypt is attempted without a user_id —
	// the user_id is mandatory because it is the GCM AAD that binds ciphertext to a row.
	ErrNoUser = errors.New("crypto: userID required")
	// ErrNoProvider is returned when an encrypted blob must be opened but no
	// KeyProvider is configured (e.g. prod misconfig). Never silently returns plaintext.
	ErrNoProvider = errors.New("crypto: encrypted data but no key provider")
)

// Encryptor is the column-level helper core's storage layer calls. It binds a
// KeyProvider with the GCM Cipher and uses the user_id as AAD so a ciphertext blob
// cannot be moved between users' rows.
//
// Nil-safe: a nil *Encryptor, or one built from a nil KeyProvider, passes data through
// in plaintext on Encrypt and reads legacy plaintext on Decrypt — the documented dev /
// no-KMS degrade path. It never panics. The one thing it will NOT do is silently treat
// an actual encrypted blob as plaintext: Decrypt of a magic-prefixed blob with no
// provider returns ErrNoProvider.
type Encryptor struct {
	provider KeyProvider
	cipher   Cipher
}

// NewEncryptor builds an Encryptor over p. p may be nil (plaintext passthrough).
// Wrap p in a CachedProvider first if unwrap is expensive.
func NewEncryptor(p KeyProvider) *Encryptor { return &Encryptor{provider: p} }

// Enabled reports whether encryption is active (a provider is wired).
func (e *Encryptor) Enabled() bool { return e != nil && e.provider != nil }

// Encrypt seals plaintext for userID. With no provider, returns plaintext unchanged
// (stored as legacy plaintext — readable later by anyone, encrypted or not).
func (e *Encryptor) Encrypt(ctx context.Context, userID string, plaintext []byte) ([]byte, error) {
	if !e.Enabled() {
		return plaintext, nil
	}
	if userID == "" {
		return nil, ErrNoUser
	}
	dek, err := e.provider.UnwrapDEK(ctx, userID)
	if err != nil {
		return nil, err
	}
	return e.cipher.Seal(dek, plaintext, []byte(userID))
}

// Decrypt opens a blob for userID. Legacy (un-prefixed) plaintext passes through, so
// reads work against a mixed store mid-backfill. A magic-prefixed blob requires a
// provider (else ErrNoProvider) and is authenticated against userID.
func (e *Encryptor) Decrypt(ctx context.Context, userID string, blob []byte) ([]byte, error) {
	if !IsEncrypted(blob) {
		return blob, nil // legacy plaintext
	}
	if !e.Enabled() {
		return nil, ErrNoProvider
	}
	if userID == "" {
		return nil, ErrNoUser
	}
	dek, err := e.provider.UnwrapDEK(ctx, userID)
	if err != nil {
		return nil, err
	}
	pt, _, err := e.cipher.Open(dek, blob, []byte(userID))
	return pt, err
}

// EncryptString is the string convenience over Encrypt. The result is binary
// ciphertext — store it in a bytea column, not a text column.
func (e *Encryptor) EncryptString(ctx context.Context, userID, s string) ([]byte, error) {
	return e.Encrypt(ctx, userID, []byte(s))
}

// DecryptString is the string convenience over Decrypt.
func (e *Encryptor) DecryptString(ctx context.Context, userID string, blob []byte) (string, error) {
	pt, err := e.Decrypt(ctx, userID, blob)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

// SealString encrypts plaintext and base64-encodes the result so it can be stored in
// an existing text/varchar/jsonb-string column WITHOUT a bytea column migration. With
// no provider it returns plaintext unchanged (no base64), so the column stays
// human-readable and backward-compatible in dev / pre-KMS environments.
func (e *Encryptor) SealString(ctx context.Context, userID, plaintext string) (string, error) {
	if !e.Enabled() {
		return plaintext, nil
	}
	blob, err := e.Encrypt(ctx, userID, []byte(plaintext))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(blob), nil
}

// OpenString reverses SealString. A value that is not valid base64, or whose decoded
// bytes lack the ciphertext magic, is treated as legacy plaintext and returned as-is —
// so reads work against a column holding a mix of encrypted and pre-encryption rows.
// An encrypted value with no provider returns ErrNoProvider (never leaks ciphertext).
func (e *Encryptor) OpenString(ctx context.Context, userID, stored string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(stored)
	if err != nil || !IsEncrypted(decoded) {
		return stored, nil // legacy plaintext
	}
	if !e.Enabled() {
		return "", ErrNoProvider
	}
	pt, derr := e.Decrypt(ctx, userID, decoded)
	if derr != nil {
		return "", derr
	}
	return string(pt), nil
}
