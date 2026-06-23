package crypto

import (
	"context"
	"errors"
	"testing"
)

func staticProvider() KeyProvider {
	dek := testKey()
	return KeyProviderFunc(func(_ context.Context, _ string) ([]byte, error) {
		return dek, nil
	})
}

func TestEncryptor_RoundTrip(t *testing.T) {
	e := NewEncryptor(staticProvider())
	ctx := context.Background()
	const msg = "everything feels pointless lately"

	blob, err := e.EncryptString(ctx, "u-1", msg)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if !IsEncrypted(blob) {
		t.Fatal("expected encrypted blob")
	}
	got, err := e.DecryptString(ctx, "u-1", blob)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if got != msg {
		t.Fatalf("round-trip mismatch: %q != %q", got, msg)
	}
}

func TestEncryptor_NilProvider_Passthrough(t *testing.T) {
	e := NewEncryptor(nil)
	ctx := context.Background()
	if e.Enabled() {
		t.Fatal("nil provider should not be enabled")
	}

	// Encrypt → plaintext passthrough.
	out, err := e.Encrypt(ctx, "u-1", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hi" || IsEncrypted(out) {
		t.Fatal("nil provider should store plaintext")
	}
	// Decrypt of plaintext → returns it.
	back, err := e.Decrypt(ctx, "u-1", out)
	if err != nil || string(back) != "hi" {
		t.Fatalf("plaintext decrypt failed: %v %q", err, back)
	}
}

func TestEncryptor_NilProvider_RefusesCiphertext(t *testing.T) {
	// Encrypt with a provider, then try to read with none — must NOT silently
	// surface ciphertext bytes; must error.
	blob, err := NewEncryptor(staticProvider()).EncryptString(context.Background(), "u-1", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewEncryptor(nil).Decrypt(context.Background(), "u-1", blob); err != ErrNoProvider {
		t.Fatalf("expected ErrNoProvider, got %v", err)
	}
}

func TestEncryptor_CrossUser_Fails(t *testing.T) {
	e := NewEncryptor(staticProvider()) // same DEK for all users
	ctx := context.Background()
	blob, _ := e.EncryptString(ctx, "u-A", "private to A")
	// Even with the same DEK, the AAD (userID) differs → auth failure.
	if _, err := e.DecryptString(ctx, "u-B", blob); err == nil {
		t.Fatal("expected cross-user decrypt to fail (AAD binding)")
	}
}

func TestEncryptor_EmptyUser(t *testing.T) {
	e := NewEncryptor(staticProvider())
	if _, err := e.Encrypt(context.Background(), "", []byte("x")); err != ErrNoUser {
		t.Fatalf("expected ErrNoUser, got %v", err)
	}
}

func TestEncryptor_SealOpenString_RoundTrip(t *testing.T) {
	e := NewEncryptor(staticProvider())
	ctx := context.Background()
	const msg = `{"mood":"low","note":"can't sleep"}`

	stored, err := e.SealString(ctx, "u-1", msg)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if stored == msg {
		t.Fatal("expected base64 ciphertext, got plaintext")
	}
	got, err := e.OpenString(ctx, "u-1", stored)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got != msg {
		t.Fatalf("round-trip mismatch: %q != %q", got, msg)
	}
}

// TestEncryptor_OpenString_NoProvider_OnCiphertext pins the contract that the
// brihasai-core dashboard read paths branch on: when a row is genuinely
// encrypted but the reader has NO key provider (KMS misconfigured / key
// unavailable), OpenString returns ErrNoProvider — NEVER leaks ciphertext and
// NEVER silently returns "". Core uses errors.Is(err, ErrNoProvider) to fail
// the request visibly (503) instead of returning blank data. A regression that
// returned "" here would re-introduce the silent-data-loss bug.
func TestEncryptor_OpenString_NoProvider_OnCiphertext(t *testing.T) {
	ctx := context.Background()
	// Seal with a provider so we have real ciphertext.
	sealed, err := NewEncryptor(staticProvider()).SealString(ctx, "u-1", "private note")
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	// Open with NO provider — must surface ErrNoProvider, not "".
	got, err := NewEncryptor(nil).OpenString(ctx, "u-1", sealed)
	if !errors.Is(err, ErrNoProvider) {
		t.Fatalf("expected ErrNoProvider opening ciphertext without provider, got err=%v", err)
	}
	if got != "" {
		t.Fatalf("must not leak ciphertext on ErrNoProvider, got %q", got)
	}
}

func TestEncryptor_OpenString_LegacyPlaintext(t *testing.T) {
	e := NewEncryptor(staticProvider())
	// A pre-encryption plaintext row (also covers accidentally-valid-base64 text).
	for _, legacy := range []string{`{"mood":"ok"}`, "test", "hello world"} {
		got, err := e.OpenString(context.Background(), "u-1", legacy)
		if err != nil {
			t.Fatalf("open legacy %q: %v", legacy, err)
		}
		if got != legacy {
			t.Fatalf("legacy passthrough failed: %q != %q", got, legacy)
		}
	}
}

func TestEncryptor_SealString_NilProvider_Passthrough(t *testing.T) {
	e := NewEncryptor(nil)
	out, err := e.SealString(context.Background(), "u-1", "plain")
	if err != nil || out != "plain" {
		t.Fatalf("nil provider should passthrough: %q %v", out, err)
	}
}
