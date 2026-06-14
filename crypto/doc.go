// Package crypto is the brihasai at-rest encryption substrate, shared by
// brihasai-core (storage layer) and brihasai-platform (Cloud KMS key custody).
//
// It contains ZERO domain IP — only AES-256-GCM authenticated encryption, a
// per-user DEK KeyProvider contract, a per-pod LRU+TTL DEK cache, and an Encryptor
// that core's storage layer calls per content column. It never learns what is being
// encrypted; it only sees (userID, bytes). That is why it can live in a repo broader
// than the founder-only core without weakening the moat.
//
// The wire format defined here — magic(2) || version(1) || nonce(12) || ciphertext+tag
// — is a CROSS-REPO CONTRACT and the single source of truth, so core and platform can
// never drift on how ciphertext is framed.
//
// Design (full spec: brihasai-core/HIGH_TRUST_ARCHITECTURE.md):
//
//   - Envelope model: a KMS master key (KEK, owned by platform) wraps a per-user DEK.
//     This package handles only the DEK→content layer; KEK custody never lives here.
//   - nil-safe: a nil Encryptor or a nil KeyProvider passes data through in plaintext
//     (the documented dev / no-KMS degrade path). Never panics, never blocks /chat.
//   - mixed-store: Open/Decrypt pass legacy un-prefixed plaintext through unchanged,
//     so reads work mid-backfill before every row is encrypted.
//   - AAD binding: the user_id is bound into the GCM tag, so a ciphertext blob cannot
//     be moved from one user's row into another's.
//   - ephemeral keys: the per-pod cache holds unwrapped DEKs in process memory only,
//     never persisted — persisting them would reunite keys with the data tier and
//     defeat the at-rest custody model.
package crypto
