//! Envelope encryption for lattice-db value bytes.
//!
//! ## Design
//!
//! - **Master key**: 32-byte key loaded from the `LDB_MASTER_KEY` environment
//!   variable (hex or base64, 32+ bytes).  In development set `LDB_DEV_SEED`
//!   instead; a deterministic key is derived via HKDF so restarts are idempotent.
//!
//! - **Per-table DEK**: derived from the master key with HKDF-SHA256 keyed on
//!   `"table:{table_name}"`.  Rotating the master key invalidates all
//!   tables simultaneously; rotating per-table requires re-encrypting one bucket.
//!
//! - **Envelope format** (bytes stored in NATS KV):
//!   `[1 byte version] [12 bytes nonce] [N bytes AES-256-GCM ciphertext+tag]`
//!
//! - **AAD**: `"{table_name}:{key}"` — binds the ciphertext to its KV location.
//!   Copying a ciphertext to a different key or table fails decryption.
//!
//! ## Default-on
//!
//! Encryption is **on by default for every table**.  The service refuses to
//! boot without a master key unless the operator explicitly opts into plaintext
//! by setting `LDB_ALLOW_PLAINTEXT=1` (or `true`/`yes`).  When plaintext is
//! permitted, an individual table may opt out via `"encrypted": false` in its
//! schema; without `LDB_ALLOW_PLAINTEXT` such schemas are rejected.
//!
//! ## Strict store authentication & migration
//!
//! A value stored in an encrypted table that fails decryption is **always**
//! treated as corruption or tampering — plaintext is never silently accepted
//! from the backing store, because that would let anyone with direct write
//! access to the NATS KV buckets (but no key) forge data the service trusts.
//!
//! Data written before encryption became the default ("legacy plaintext") is
//! migrated only during an explicit, temporary migration window enabled with
//! `LDB_MIGRATE_PLAINTEXT=1`.  While active, values that fail decryption and
//! do not [`looks_like_envelope`] are read as legacy plaintext and immediately
//! re-encrypted back to KV (read-repair at table-load time), so each value is
//! accepted as plaintext at most once.  Remove the flag when the logs show no
//! remaining legacy values.

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

/// First set value among equivalent env var aliases.
fn env_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|k| std::env::var(k).ok())
}

/// Load and return the master key, or panic if none is configured.
///
/// Caching is intentionally left to the caller (call once at startup).
pub fn load_master_key() -> [u8; 32] {
    try_load_master_key().expect(
        "no master key is configured. Set JETCACHE_MASTER_KEY / LDB_MASTER_KEY (production) or \
         JETCACHE_DEV_SEED / LDB_DEV_SEED (development only), or set LDB_ALLOW_PLAINTEXT=1 to \
         explicitly run without encryption.",
    )
}

/// Load the master key if one is configured.
///
/// Returns `None` when no key/seed env var is set.  Panics when a master key
/// is set but malformed or too short — a malformed key is always an operator
/// error, never a reason to fall back to plaintext.
pub fn try_load_master_key() -> Option<[u8; 32]> {
    // Production path: JETCACHE_MASTER_KEY / CACHE_MASTER_KEY / LDB_MASTER_KEY as hex or base64, must be 32+ bytes.
    let master_env = env_any(&["JETCACHE_MASTER_KEY", "CACHE_MASTER_KEY", "LDB_MASTER_KEY"]);
    if let Some(raw) = master_env {
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
        return Some(key);
    }

    // Dev path: JETCACHE_DEV_SEED / CACHE_DEV_SEED / LDB_DEV_SEED — deterministic HKDF-derived key.
    let dev_seed_env = env_any(&["JETCACHE_DEV_SEED", "CACHE_DEV_SEED", "LDB_DEV_SEED"]);
    if let Some(seed) = dev_seed_env {
        eprintln!(
            "jetcache: WARNING — using JETCACHE_DEV_SEED / CACHE_DEV_SEED / LDB_DEV_SEED for encryption. \
             Never use this in production. Set JETCACHE_MASTER_KEY instead."
        );
        return Some(derive_dev_key(&seed));
    }

    None
}

/// True when the named plaintext-permission env var (any alias) is set to a
/// truthy value (`1`, `true`, `yes`).
fn flag_permitted(keys: &[&str]) -> bool {
    env_any(keys)
        .map(|v| matches!(v.trim(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

/// True when the operator has explicitly opted into plaintext storage.
pub fn plaintext_permitted() -> bool {
    flag_permitted(&[
        "JETCACHE_ALLOW_PLAINTEXT",
        "CACHE_ALLOW_PLAINTEXT",
        "LDB_ALLOW_PLAINTEXT",
    ])
}

/// True when the operator has explicitly enabled the legacy-plaintext
/// migration window.  While active, pre-encryption plaintext values in
/// encrypted tables are readable and re-encrypted back to KV on load.  This
/// weakens store authentication (see module docs) and must only be set
/// temporarily during a migration.
pub fn migration_permitted() -> bool {
    flag_permitted(&[
        "JETCACHE_MIGRATE_PLAINTEXT",
        "CACHE_MIGRATE_PLAINTEXT",
        "LDB_MIGRATE_PLAINTEXT",
    ])
}

/// True when `bytes` has the shape of an envelope produced by [`encrypt`]
/// (version byte + minimum length).  Used to distinguish legacy plaintext
/// values from real ciphertext on the read path; a value that looks like an
/// envelope but fails AEAD verification is treated as corruption/tampering,
/// never as plaintext.
pub fn looks_like_envelope(bytes: &[u8]) -> bool {
    bytes.len() >= MIN_ENVELOPE_LEN && bytes[0] == VERSION
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

    #[test]
    fn envelope_detection() {
        let master = test_master();
        let nonce = [7u8; 12];
        let envelope = encrypt_with_nonce(&master, "users", "user-1", b"secret", &nonce);
        assert!(looks_like_envelope(&envelope));

        // Typical plaintext values are not envelopes.
        assert!(!looks_like_envelope(b""));
        assert!(!looks_like_envelope(b"short"));
        assert!(!looks_like_envelope(
            b"{\"a\":\"json-object-long-enough-to-exceed-min-envelope-len\"}"
        ));
    }
}
