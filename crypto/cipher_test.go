package crypto

import (
	"bytes"
	"testing"
)

func testKey() []byte {
	k := make([]byte, KeySize)
	for i := range k {
		k[i] = byte(i + 1)
	}
	return k
}

func TestCipher_SealOpen_RoundTrip(t *testing.T) {
	var c Cipher
	dek := testKey()
	aad := []byte("u-123")
	plain := []byte("I haven't slept properly in weeks.")

	blob, err := c.Seal(dek, plain, aad)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if !IsEncrypted(blob) {
		t.Fatal("blob is not marked encrypted")
	}
	if bytes.Contains(blob, plain) {
		t.Fatal("plaintext leaked into ciphertext")
	}

	got, enc, err := c.Open(dek, blob, aad)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !enc {
		t.Fatal("expected encrypted=true")
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round-trip mismatch: %q != %q", got, plain)
	}
}

func TestCipher_Open_LegacyPlaintextPassthrough(t *testing.T) {
	var c Cipher
	legacy := []byte("plain old conversation row")
	got, enc, err := c.Open(testKey(), legacy, []byte("u-1"))
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if enc {
		t.Fatal("legacy plaintext reported as encrypted")
	}
	if !bytes.Equal(got, legacy) {
		t.Fatal("legacy plaintext not returned unchanged")
	}
}

func TestCipher_Open_WrongKey(t *testing.T) {
	var c Cipher
	blob, _ := c.Seal(testKey(), []byte("secret"), []byte("u-1"))
	wrong := make([]byte, KeySize) // all-zero
	if _, _, err := c.Open(wrong, blob, []byte("u-1")); err == nil {
		t.Fatal("expected auth failure with wrong key")
	}
}

func TestCipher_Open_AADMismatch(t *testing.T) {
	var c Cipher
	blob, _ := c.Seal(testKey(), []byte("secret"), []byte("u-A"))
	if _, _, err := c.Open(testKey(), blob, []byte("u-B")); err == nil {
		t.Fatal("expected auth failure when AAD (userID) differs")
	}
}

func TestCipher_Seal_BadKeySize(t *testing.T) {
	var c Cipher
	if _, err := c.Seal([]byte("short"), []byte("x"), nil); err != ErrKeySize {
		t.Fatalf("expected ErrKeySize, got %v", err)
	}
}

func TestCipher_Open_Tampered(t *testing.T) {
	var c Cipher
	blob, _ := c.Seal(testKey(), []byte("secret"), []byte("u-1"))
	blob[len(blob)-1] ^= 0xFF // flip a tag bit
	if _, _, err := c.Open(testKey(), blob, []byte("u-1")); err == nil {
		t.Fatal("expected auth failure on tampered ciphertext")
	}
}

func TestIsEncrypted(t *testing.T) {
	if IsEncrypted([]byte("hello")) {
		t.Fatal("plaintext flagged encrypted")
	}
	if IsEncrypted(nil) {
		t.Fatal("nil flagged encrypted")
	}
	blob, _ := Cipher{}.Seal(testKey(), []byte("x"), nil)
	if !IsEncrypted(blob) {
		t.Fatal("ciphertext not flagged encrypted")
	}
}
