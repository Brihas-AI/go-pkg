package crypto

import "context"

// KeyProvider yields the plaintext per-user data encryption key (DEK). It is the only
// contract core needs: platform implements it (Cloud KMS envelope unwrap of the
// wrapped DEK held in the user_keys table), core consumes it via Encryptor.
//
// Implementations MUST be safe for concurrent use and SHOULD be cheap to call
// repeatedly (wrap an expensive provider in CachedProvider). A non-nil error means
// the DEK could not be produced — callers must treat that as "cannot read/write this
// user's encrypted content right now", never as plaintext.
type KeyProvider interface {
	UnwrapDEK(ctx context.Context, userID string) ([]byte, error)
}

// DEKStore persists KMS-wrapped per-user Data Encryption Keys. The wrapped bytes are
// opaque ciphertext — only the KEK can produce the plaintext DEK. Implementations
// must be safe for concurrent use.
type DEKStore interface {
	// Get returns the wrapped DEK for userID. found=false means no row exists yet.
	Get(ctx context.Context, userID string) (wrapped []byte, found bool, err error)
	// PutIfAbsent stores wrapped for userID if no row exists yet. Returns the
	// canonical stored bytes — may differ from the input if another writer raced us
	// (caller must use the returned value, not the one it passed in).
	PutIfAbsent(ctx context.Context, userID string, wrapped []byte) (stored []byte, err error)
}

// KeyProviderFunc adapts an ordinary function to a KeyProvider — handy for tests and
// for a 768d/3072d-style closure that already has the unwrap logic inline.
type KeyProviderFunc func(ctx context.Context, userID string) ([]byte, error)

// UnwrapDEK implements KeyProvider.
func (f KeyProviderFunc) UnwrapDEK(ctx context.Context, userID string) ([]byte, error) {
	return f(ctx, userID)
}
