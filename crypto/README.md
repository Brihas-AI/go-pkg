# crypto — brihasai at-rest encryption substrate

Shared, **zero-IP** encryption primitives used by **brihasai-core** (storage layer) and
**brihasai-platform** (Cloud KMS key custody). Full design:
`brihasai-core/HIGH_TRUST_ARCHITECTURE.md`.

This package sees only `(userID, bytes)` — it never learns what is encrypted or how the
domain works, so it is safe to host outside the founder-only core. It is the **single
source of truth for the ciphertext wire format**, so core and platform can't drift.

## Wire format

```
magic(2) || version(1) || nonce(12) || ciphertext+tag
0xB1 0x15 | 0x01 (GCM) | random      | AES-256-GCM, AAD = user_id
```

- `0xB1` is a UTF-8 continuation byte → valid text plaintext can never start with it, so
  `Open` transparently passes **legacy un-prefixed plaintext** through unchanged. This is
  what makes mixed-store reads work during the backfill.
- The **user_id is the GCM AAD** — a ciphertext blob authenticated for user A fails to
  open for user B, so rows can't be swapped between users.

## API surface

| Type / func | Role |
|---|---|
| `KeyProvider` | interface: `UnwrapDEK(ctx, userID) ([]byte, error)` — platform implements (KMS) |
| `KeyProviderFunc` | func adapter for the interface |
| `Cipher` | `Seal(dek, plaintext, aad)` / `Open(dek, blob, aad)` — AES-256-GCM codec |
| `CachedProvider` | per-pod LRU+TTL + singleflight wrapper over a `KeyProvider` |
| `Encryptor` | column-level `Encrypt/Decrypt(ctx, userID, …)` core's storage calls |
| `IsEncrypted(blob)` | detect the magic prefix (encrypted vs legacy plaintext) |

## Invariants

- **nil-safe:** a nil `*Encryptor` or nil `KeyProvider` → plaintext passthrough (dev /
  no-KMS). Never panics, never blocks `/chat`. It will **not** silently surface a real
  ciphertext blob as plaintext — `Decrypt` of an encrypted blob with no provider returns
  `ErrNoProvider`.
- **DEK is ephemeral:** `CachedProvider` holds unwrapped DEKs in process memory only —
  **never persist them** to Redis/disk (that reunites keys with the data tier).
- `KeySize == 32` (AES-256). `UnwrapDEK` must return a 32-byte key.

## Wiring

**platform** (owns custody) — implement `KeyProvider` over Cloud KMS, wrap in the cache:

```go
kms := keys.NewKMSKeyProvider(kmsClient, kekResourceName) // platform internal/keys
provider := crypto.NewCachedProvider(kms, crypto.DefaultCacheCapacity, crypto.DefaultCacheTTL)
// inject `provider` into core via Deps.Keys
```

**core** (owns policy — which columns, when) — one `Encryptor`, used at the storage edge:

```go
enc := crypto.NewEncryptor(deps.Keys) // deps.Keys may be nil in dev → plaintext

ct, err := enc.EncryptString(ctx, userID, msg)     // on write
pt, err := enc.DecryptString(ctx, userID, ctBytes) // on read (legacy plaintext passes through)
```

## Test

```bash
go test -race -count=1 ./crypto/...
```
