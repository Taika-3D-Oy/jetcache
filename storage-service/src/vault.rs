//! Envelope encryption for lattice-db value bytes.
//!
//! ## Design
//!
//! - **Master key**: 32-byte key loaded from the `LDB_MASTER_KEY` environment
//!   variable (hex or base64, 32+ bytes).  In development set `LDB_DEV_SEED`
//!   instead; a deterministic key is derived via HKDF so restarts are idempotent.
//!   If neither is set and `LDB_ENCRYPTION_MODE != off`, the process panics on
//!   first use so misconfiguration is caught at startup.
//!
//! - **Per-table DEK**: derived from the master key with HKDF-SHA256 keyed on
//!   `"lattice-db-dek:{table_name}"`.  Rotating the master key invalidates all
//!   tables simultaneously; rotating per-table requires re-encrypting one bucket.
//!
//! - **Envelope format** (bytes stored in NATS KV):
//!   `[1 byte version] [12 bytes nonce] [N bytes AES-256-GCM ciphertext+tag]`
//!
//! - **AAD**: `"{table_name}:{key}"` — binds the ciphertext to its KV location.
//!   Copying a ciphertext to a different key or table fails decryption.
//!
//! ## Opt-in
//!
//! Encryption is per-table.  A table is encrypted when its schema contains
//! `"encrypted": true`.  Tables without this flag (or with no schema) are stored
//! and retrieved as plaintext even when a master key is configured.

use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Key, Nonce,
};
use hkdf::Hkdf;
use sha2::Sha256;

// ── Constants ─────────────────────────────────────────────────────────────────

const VERSION: u8 = 1;
const NONCE_LEN: usize = 12;
/// Minimum valid envelope: 1 (version) + 12 (nonce) + 16 (AES-GCM tag, empty PT).
const MIN_ENVELOPE_LEN: usize = 1 + NONCE_LEN + 16;

// ── Master key loading ────────────────────────────────────────────────────────

/// Load and return the master key, or panic if misconfigured.
///
/// Caching is intentionally left to the caller (call once at startup).
pub fn load_master_key() -> [u8; 32] {
    // Production path: JETCACHE_MASTER_KEY / CACHE_MASTER_KEY / LDB_MASTER_KEY as hex or base64, must be 32+ bytes.
    let master_env = std::env::var("JETCACHE_MASTER_KEY")
        .or_else(|_| std::env::var("CACHE_MASTER_KEY"))
        .or_else(|_| std::env::var("LDB_MASTER_KEY"));
    if let Ok(raw) = master_env {
        let raw = raw.trim().to_string();
        // Try hex first, then base64.
        let bytes = if raw.len() >= 64 && raw.chars().all(|c| c.is_ascii_hexdigit()) {
            hex_decode(&raw)
        } else {
            base64_decode(&raw)
        };
        let bytes = bytes.expect(
            "JETCACHE_MASTER_KEY / CACHE_MASTER_KEY / LDB_MASTER_KEY must be a hex (64+ chars) or base64-encoded value of at least 32 bytes",
        );
        if bytes.len() < 32 {
            panic!(
                "JETCACHE_MASTER_KEY / CACHE_MASTER_KEY / LDB_MASTER_KEY must be at least 32 bytes (got {})",
                bytes.len()
            );
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes[..32]);
        return key;
    }

    // Dev path: JETCACHE_DEV_SEED / CACHE_DEV_SEED / LDB_DEV_SEED — deterministic HKDF-derived key.
    let dev_seed_env = std::env::var("JETCACHE_DEV_SEED")
        .or_else(|_| std::env::var("CACHE_DEV_SEED"))
        .or_else(|_| std::env::var("LDB_DEV_SEED"));
    if let Ok(seed) = dev_seed_env {
        eprintln!(
            "jetcache: WARNING — using JETCACHE_DEV_SEED / CACHE_DEV_SEED / LDB_DEV_SEED for encryption. \
             Never use this in production. Set JETCACHE_MASTER_KEY instead."
        );
        return derive_dev_key(&seed);
    }

    // Neither set: fail fast.
    panic!(
        "jetcache: encryption is enabled for one or more tables but no master key is \
         configured. Set JETCACHE_MASTER_KEY (production) or JETCACHE_DEV_SEED (development only)."
    );
}

fn derive_dev_key(seed: &str) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(b"lattice-db-dev-vault-v1"), seed.as_bytes());
    let mut key = [0u8; 32];
    hk.expand(b"master-key", &mut key).expect("hkdf expand");
    key
}

// ── Table DEK derivation ──────────────────────────────────────────────────────

fn derive_table_dek(master: &[u8; 32], table: &str) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(b"lattice-db-dek-v1"), master);
    let mut dek = [0u8; 32];
    hk.expand(format!("table:{table}").as_bytes(), &mut dek)
        .expect("hkdf expand");
    dek
}

// ── Encrypt / Decrypt ─────────────────────────────────────────────────────────

/// Encrypt `plaintext` for `(table, key)` using the given master key.
/// Returns the envelope bytes to be stored in NATS KV.
pub fn encrypt(master: &[u8; 32], table: &str, kv_key: &str, plaintext: &[u8]) -> Vec<u8> {
    let dek = derive_table_dek(master, table);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));

    let mut nonce_bytes = [0u8; NONCE_LEN];
    let rand_bytes = wasip3::random::random::get_random_bytes(NONCE_LEN as u64);
    nonce_bytes.copy_from_slice(&rand_bytes);

    let aad = format!("{table}:{kv_key}");
    let payload = Payload {
        msg: plaintext,
        aad: aad.as_bytes(),
    };
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), payload)
        .expect("AES-GCM encrypt");

    // [1 byte version] [12 bytes nonce] [ciphertext + 16 byte tag]
    let mut envelope = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
    envelope.push(VERSION);
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend_from_slice(&ciphertext);
    envelope
}

/// Decrypt an envelope produced by [`encrypt`].
///
/// Returns `Err` if the envelope is malformed, the version is unknown, or the
/// AAD does not match (wrong table/key or tampered data).
pub fn decrypt(
    master: &[u8; 32],
    table: &str,
    kv_key: &str,
    envelope: &[u8],
) -> Result<Vec<u8>, String> {
    if envelope.len() < MIN_ENVELOPE_LEN {
        return Err(format!(
            "ciphertext too short: {} bytes (min {})",
            envelope.len(),
            MIN_ENVELOPE_LEN
        ));
    }

    let version = envelope[0];
    if version != VERSION {
        return Err(format!("unsupported envelope version: {version}"));
    }

    let nonce_bytes: [u8; NONCE_LEN] = envelope[1..1 + NONCE_LEN]
        .try_into()
        .expect("slice has correct len");
    let ciphertext = &envelope[1 + NONCE_LEN..];

    let dek = derive_table_dek(master, table);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));

    let aad = format!("{table}:{kv_key}");
    let payload = Payload {
        msg: ciphertext,
        aad: aad.as_bytes(),
    };
    cipher
        .decrypt(Nonce::from_slice(&nonce_bytes), payload)
        .map_err(|_| format!("decryption failed for {table}:{kv_key} (AAD mismatch or corruption)"))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for chunk in s.as_bytes().chunks(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    Some(bytes)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .ok()
        .or_else(|| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(s)
                .ok()
        })
}

// ── Unit tests ────────────────────────────────────────────────────────────────

// These tests run on the wasm target where wasip3::random is available.
// Native unit tests only cover the deterministic helpers.
#[cfg(test)]
mod tests {
    use super::*;

    fn test_master() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    /// Encrypt with a fixed nonce (test helper — not for production).
    fn encrypt_with_nonce(
        master: &[u8; 32],
        table: &str,
        kv_key: &str,
        plaintext: &[u8],
        nonce_bytes: &[u8; 12],
    ) -> Vec<u8> {
        let dek = derive_table_dek(master, table);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));
        let aad = format!("{table}:{kv_key}");
        let payload = aes_gcm::aead::Payload {
            msg: plaintext,
            aad: aad.as_bytes(),
        };
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(nonce_bytes), payload)
            .unwrap();
        let mut envelope = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        envelope.push(VERSION);
        envelope.extend_from_slice(nonce_bytes);
        envelope.extend_from_slice(&ciphertext);
        envelope
    }

    #[test]
    fn round_trip() {
        let master = test_master();
        let nonce = [42u8; 12];
        let pt = b"hello world";
        let envelope = encrypt_with_nonce(&master, "users", "user-1", pt, &nonce);
        let decrypted = decrypt(&master, "users", "user-1", &envelope).unwrap();
        assert_eq!(decrypted, pt);
    }

    #[test]
    fn wrong_table_fails() {
        let master = test_master();
        let nonce = [1u8; 12];
        let envelope = encrypt_with_nonce(&master, "users", "user-1", b"secret", &nonce);
        assert!(decrypt(&master, "sessions", "user-1", &envelope).is_err());
    }

    #[test]
    fn wrong_key_fails() {
        let master = test_master();
        let nonce = [2u8; 12];
        let envelope = encrypt_with_nonce(&master, "users", "user-1", b"secret", &nonce);
        assert!(decrypt(&master, "users", "user-2", &envelope).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let master = test_master();
        let nonce = [3u8; 12];
        let mut envelope = encrypt_with_nonce(&master, "users", "user-1", b"secret", &nonce);
        let last = envelope.len() - 1;
        envelope[last] ^= 0xff;
        assert!(decrypt(&master, "users", "user-1", &envelope).is_err());
    }

    #[test]
    fn dev_key_deterministic() {
        let k1 = derive_dev_key("my-seed");
        let k2 = derive_dev_key("my-seed");
        assert_eq!(k1, k2);
    }
}
