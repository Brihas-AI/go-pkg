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

// KeyProviderFunc adapts an ordinary function to a KeyProvider — handy for tests and
// for a 768d/3072d-style closure that already has the unwrap logic inline.
type KeyProviderFunc func(ctx context.Context, userID string) ([]byte, error)

// UnwrapDEK implements KeyProvider.
func (f KeyProviderFunc) UnwrapDEK(ctx context.Context, userID string) ([]byte, error) {
	return f(ctx, userID)
}
