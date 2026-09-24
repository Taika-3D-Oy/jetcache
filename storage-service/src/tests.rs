//! Unit tests for storage-service critical functionality.

#[cfg(test)]
mod tests {
    use crate::state::TableState;

    // ──────────────────────────────────────────────────────────────────
    // In-memory TableState & Prefix Tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_table_state_upsert_and_get() {
        let mut table = TableState::new();
        table.upsert("user:101", b"{\"name\":\"alice\"}".to_vec(), 1);
        table.upsert("user:102", b"{\"name\":\"bob\"}".to_vec(), 2);
        table.upsert("session:abc", b"{\"uid\":\"101\"}".to_vec(), 3);

        assert_eq!(table.data.len(), 3);
        assert_eq!(table.data.get("user:101").unwrap().revision, 1);
        assert_eq!(table.data.get("user:102").unwrap().revision, 2);
        assert_eq!(table.data.get("session:abc").unwrap().revision, 3);
    }

    #[test]
    fn test_table_state_delete() {
        let mut table = TableState::new();
        table.upsert("k1", b"v1".to_vec(), 1);
        assert!(table.data.contains_key("k1"));

        table.remove("k1");
        table.note_applied_revision(2);
        assert!(!table.data.contains_key("k1"));
        assert_eq!(table.applied_revision, 2);
    }

    #[test]
    fn test_prefix_filtering() {
        let mut table = TableState::new();
        table.upsert("refresh_idx:u1:t1", b"v1".to_vec(), 1);
        table.upsert("refresh_idx:u1:t2", b"v2".to_vec(), 2);
        table.upsert("refresh_idx:u2:t3", b"v3".to_vec(), 3);
        table.upsert("user:u1", b"user_data".to_vec(), 4);

        let prefix = "refresh_idx:u1:";
        let matches: Vec<String> = table
            .data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();

        assert_eq!(matches.len(), 2);
        assert!(matches.contains(&"refresh_idx:u1:t1".to_string()));
        assert!(matches.contains(&"refresh_idx:u1:t2".to_string()));
    }

    // ──────────────────────────────────────────────────────────────────
    // JSON Logging Tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_json_escape_quotes() {
        let input = r#"message with "quotes""#;
        let output = crate::log::escape_json_string(input);
        assert!(output.contains(r#"\""#));
    }

    #[test]
    fn test_json_escape_newlines() {
        let input = "message with\nnewline";
        let output = crate::log::escape_json_string(input);
        assert!(output.contains(r#"\n"#));
    }

    #[test]
    fn test_json_escape_tabs() {
        let input = "message\twith\ttabs";
        let output = crate::log::escape_json_string(input);
        assert!(output.contains(r#"\t"#));
    }

    // ──────────────────────────────────────────────────────────────────
    // TTL Configuration Tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_ttl_seconds_to_duration() {
        use std::time::Duration;
        let ttl_secs = 30u64;
        let duration = Duration::from_secs(ttl_secs);
        assert_eq!(duration.as_secs(), 30);
    }

    #[test]
    fn test_ttl_zero_handling() {
        use std::time::Duration;
        let ttl_secs = 0u64;
        let duration = Duration::from_secs(ttl_secs);
        assert_eq!(duration.as_secs(), 0);
    }

    #[test]
    fn test_ttl_large_value() {
        use std::time::Duration;
        let ttl_secs = 86400u64; // 1 day
        let duration = Duration::from_secs(ttl_secs);
        assert_eq!(duration.as_secs(), 86400);
    }

    // ──────────────────────────────────────────────────────────────────
    // Security Fix Tests (S-01, S-03, S-04, S-08)
    // ──────────────────────────────────────────────────────────────────

    // S-01: reserved table name rejection

    #[test]
    fn test_reserved_table_rejected() {
        let payload = br#"{"table":"_indexes","key":"foo","value":"e30="}"#;
        let result = crate::handler::check_no_reserved_tables("put", payload);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("reserved"));
    }

    #[test]
    fn test_reserved_table_schemas_rejected() {
        let payload = br#"{"table":"_schemas","key":"users"}"#;
        let result = crate::handler::check_no_reserved_tables("get", payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_normal_table_allowed() {
        let payload = br#"{"table":"users","key":"alice","value":"e30="}"#;
        let result = crate::handler::check_no_reserved_tables("put", payload);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_catalog_allowed() {
        let payload = br#"{"table":"_sql_catalog","key":"users","value":"e30="}"#;
        let result = crate::handler::check_no_reserved_tables("put", payload);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_reserved_table() {
        assert!(crate::handler::is_reserved_table("_schemas"));
        assert!(crate::handler::is_reserved_table("_indexes"));
        assert!(crate::handler::is_reserved_table("_meta"));
        assert!(!crate::handler::is_reserved_table("_sql_catalog"));
        assert!(!crate::handler::is_reserved_table("users"));
    }

    // S-03: constant-time auth comparison

    #[test]
    fn test_ct_eq_equal() {
        assert!(crate::handler::ct_eq(b"secret-token", b"secret-token"));
    }

    #[test]
    fn test_ct_eq_different() {
        assert!(!crate::handler::ct_eq(b"secret-token", b"wrong-token!"));
    }

    #[test]
    fn test_ct_eq_different_lengths() {
        assert!(!crate::handler::ct_eq(b"short", b"much-longer-token"));
    }

    #[test]
    fn test_ct_eq_empty() {
        assert!(crate::handler::ct_eq(b"", b""));
        assert!(!crate::handler::ct_eq(b"", b"x"));
    }

    // S-04: write bounds validation

    #[test]
    fn test_validate_write_bounds_ok() {
        let result = crate::handler::validate_write_bounds("users", "alice", &[0u8; 100]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_write_bounds_key_too_long() {
        let long_key = "k".repeat(257);
        let result = crate::handler::validate_write_bounds("users", &long_key, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("key exceeds"));
    }

    #[test]
    fn test_validate_write_bounds_value_too_large() {
        let big_value = vec![0u8; 1 * 1024 * 1024 + 1];
        let result = crate::handler::validate_write_bounds("users", "key", &big_value);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("value exceeds"));
    }

    #[test]
    fn test_validate_write_bounds_table_too_long() {
        let long_table = "t".repeat(129);
        let result = crate::handler::validate_write_bounds(&long_table, "key", &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("table name exceeds"));
    }

    #[test]
    fn test_validate_write_bounds_exactly_at_limits() {
        let max_key = "k".repeat(256);
        let max_value = vec![0u8; 1 * 1024 * 1024];
        let result = crate::handler::validate_write_bounds("users", &max_key, &max_value);
        assert!(result.is_ok());
    }

    // S-08: control character escaping in JSON logs

    #[test]
    fn test_json_escape_control_chars_nul() {
        let input = "before\x00after";
        let output = crate::log::escape_json_string(input);
        assert!(!output.contains('\x00'), "NUL must be escaped");
        assert!(output.contains("\\u0000"));
    }

    #[test]
    fn test_json_escape_control_chars_bell() {
        let input = "ring\x07bell";
        let output = crate::log::escape_json_string(input);
        assert!(!output.contains('\x07'));
        assert!(output.contains("\\u0007"));
    }

    #[test]
    fn test_json_escape_control_chars_escape_seq() {
        let input = "\x1b[31mred\x1b[0m";
        let output = crate::log::escape_json_string(input);
        assert!(!output.contains('\x1b'));
        assert!(output.contains("\\u001b"));
    }

    #[test]
    fn test_json_escape_known_chars_still_work() {
        let input = "a\"b\\c\nd\re\tf";
        let output = crate::log::escape_json_string(input);
        assert!(output.contains("\\\""));
        assert!(output.contains("\\\\"));
        assert!(output.contains("\\n"));
        assert!(output.contains("\\r"));
        assert!(output.contains("\\t"));
    }
}
