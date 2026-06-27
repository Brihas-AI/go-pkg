package crypto

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
)

// KEK is a Key Encryption Key that wraps and unwraps per-user DEKs.
// The production implementation is GCPKEK (Cloud KMS). Test doubles implement this
// interface directly so no real KMS credentials are needed in tests.
type KEK interface {
	// Wrap encrypts a plaintext DEK and returns the wrapped (ciphertext) bytes.
	Wrap(ctx context.Context, plainDEK []byte) (wrappedDEK []byte, err error)
	// Unwrap decrypts wrapped DEK bytes and returns the plaintext DEK.
	Unwrap(ctx context.Context, wrappedDEK []byte) (plainDEK []byte, err error)
	// Close releases any resources held by the KEK (e.g. the KMS gRPC client).
	Close() error
}

// gcpKEK is the Cloud KMS implementation of KEK. It calls the GCP KMS
// Encrypt/Decrypt API to wrap and unwrap per-user DEKs.
type gcpKEK struct {
	client  *kms.KeyManagementClient
	keyName string // projects/{p}/locations/{l}/keyRings/{r}/cryptoKeys/{k}
}

// NewGCPKEK creates a Cloud KMS KEK backed by the named GCP KMS symmetric key.
// keyName format: projects/{project}/locations/{location}/keyRings/{ring}/cryptoKeys/{key}
// The caller must call Close() on the returned KEK when done.
func NewGCPKEK(ctx context.Context, keyName string) (KEK, error) {
	client, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("crypto: GCP KMS client: %w", err)
	}
	return &gcpKEK{client: client, keyName: keyName}, nil
}

func (k *gcpKEK) Wrap(ctx context.Context, plainDEK []byte) ([]byte, error) {
	resp, err := k.client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:      k.keyName,
		Plaintext: plainDEK,
	})
	if err != nil {
		return nil, fmt.Errorf("crypto: KMS wrap: %w", err)
	}
	return resp.Ciphertext, nil
}

func (k *gcpKEK) Unwrap(ctx context.Context, wrappedDEK []byte) ([]byte, error) {
	resp, err := k.client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:       k.keyName,
		Ciphertext: wrappedDEK,
	})
	if err != nil {
		return nil, fmt.Errorf("crypto: KMS unwrap: %w", err)
	}
	return resp.Plaintext, nil
}

func (k *gcpKEK) Close() error { return k.client.Close() }

// KMSProvider is a KeyProvider that resolves per-user DEKs via a KEK + DEKStore.
//
// On first access for a user it mints a fresh 256-bit DEK, wraps it with the KEK,
// and stores the wrapped bytes via DEKStore.PutIfAbsent. Concurrent first-access
// races are safe: PutIfAbsent is atomic and the loser reads back the winner's row.
// On all subsequent accesses it reads the stored wrapped DEK and unwraps it.
//
// Wrap KMSProvider in a CachedProvider to avoid a KMS round-trip on every message.
type KMSProvider struct {
	kek   KEK
	store DEKStore
	log   *slog.Logger
}

// NewKMSProvider builds a KMSProvider. Neither kek nor store may be nil.
func NewKMSProvider(kek KEK, store DEKStore, log *slog.Logger) *KMSProvider {
	return &KMSProvider{kek: kek, store: store, log: log}
}

// UnwrapDEK implements KeyProvider.
func (p *KMSProvider) UnwrapDEK(ctx context.Context, userID string) ([]byte, error) {
	wrapped, found, err := p.store.Get(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("crypto: DEKStore.Get(%s): %w", userID, err)
	}

	if !found {
		// First access — mint a fresh DEK, wrap it, store it.
		plain := make([]byte, KeySize)
		if _, err := io.ReadFull(rand.Reader, plain); err != nil {
			return nil, fmt.Errorf("crypto: new DEK: %w", err)
		}
		wrapped, err = p.kek.Wrap(ctx, plain)
		if err != nil {
			return nil, err
		}
		stored, err := p.store.PutIfAbsent(ctx, userID, wrapped)
		if err != nil {
			return nil, fmt.Errorf("crypto: DEKStore.PutIfAbsent(%s): %w", userID, err)
		}
		// We won the race — the plain DEK we just generated is canonical.
		if string(stored) == string(wrapped) {
			return plain, nil
		}
		// Another pod won the race — use their stored wrapped DEK.
		wrapped = stored
	}

	return p.kek.Unwrap(ctx, wrapped)
}

// Close releases the KEK client. Call this when the provider is no longer needed.
func (p *KMSProvider) Close() error { return p.kek.Close() }
