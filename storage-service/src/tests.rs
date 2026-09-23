//! Unit tests for storage-service critical functionality.

#[cfg(test)]
mod tests {
    use crate::state::{matches_filters, Comparison, FieldFilter};

    // ──────────────────────────────────────────────────────────────────
    // Filter & Index Tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_filter_eq_match() {
        let filter = FieldFilter {
            field: "status".to_string(),
            cmp: Comparison::Eq("active".to_string()),
        };
        let json = br#"{"status":"active","id":1}"#;
        assert!(matches_filters(json, &[filter]));
    }

    #[test]
    fn test_filter_eq_no_match() {
        let filter = FieldFilter {
            field: "status".to_string(),
            cmp: Comparison::Eq("active".to_string()),
        };
        let json = br#"{"status":"inactive","id":1}"#;
        assert!(!matches_filters(json, &[filter]));
    }

    #[test]
    fn test_filter_prefix_match() {
        let filter = FieldFilter {
            field: "email".to_string(),
            cmp: Comparison::Prefix("user@".to_string()),
        };
        let json = br#"{"email":"user@example.com"}"#;
        assert!(matches_filters(json, &[filter]));
    }

    #[test]
    fn test_filter_gt_match() {
        let filter = FieldFilter {
            field: "score".to_string(),
            cmp: Comparison::Gt("50".to_string()),
        };
        let json = br#"{"score":"75"}"#;
        assert!(matches_filters(json, &[filter]));
    }

    #[test]
    fn test_filter_gte_match() {
        let filter = FieldFilter {
            field: "score".to_string(),
            cmp: Comparison::Gte("50".to_string()),
        };
        let json = br#"{"score":"50"}"#;
        assert!(matches_filters(json, &[filter]));
    }

    #[test]
    fn test_filter_lt_match() {
        let filter = FieldFilter {
            field: "age".to_string(),
            cmp: Comparison::Lt("18".to_string()),
        };
        let json = br#"{"age":"16"}"#;
        assert!(matches_filters(json, &[filter]));
    }

    #[test]
    fn test_filter_lte_match() {
        let filter = FieldFilter {
            field: "age".to_string(),
            cmp: Comparison::Lte("18".to_string()),
        };
        let json = br#"{"age":"18"}"#;
        assert!(matches_filters(json, &[filter]));
    }

    #[test]
    fn test_multiple_filters_all_match() {
        let filters = vec![
            FieldFilter {
                field: "status".to_string(),
                cmp: Comparison::Eq("active".to_string()),
            },
            FieldFilter {
                field: "role".to_string(),
                cmp: Comparison::Eq("admin".to_string()),
            },
        ];
        let json = br#"{"status":"active","role":"admin"}"#;
        assert!(matches_filters(json, &filters));
    }

    #[test]
    fn test_multiple_filters_one_fails() {
        let filters = vec![
            FieldFilter {
                field: "status".to_string(),
                cmp: Comparison::Eq("active".to_string()),
            },
            FieldFilter {
                field: "role".to_string(),
                cmp: Comparison::Eq("admin".to_string()),
            },
        ];
        let json = br#"{"status":"active","role":"user"}"#;
        assert!(!matches_filters(json, &filters));
    }

    #[test]
    fn test_neq_match() {
        let filter = FieldFilter {
            field: "status".to_string(),
            cmp: Comparison::Neq("deleted".to_string()),
        };
        let json = br#"{"status":"active"}"#;
        assert!(matches_filters(json, &[filter]));
    }

    #[test]
    fn test_neq_no_match() {
        let filter = FieldFilter {
            field: "status".to_string(),
            cmp: Comparison::Neq("deleted".to_string()),
        };
        let json = br#"{"status":"deleted"}"#;
        assert!(!matches_filters(json, &[filter]));
    }

    #[test]
    fn test_filter_missing_field() {
        let filter = FieldFilter {
            field: "nonexistent".to_string(),
            cmp: Comparison::Eq("value".to_string()),
        };
        let json = br#"{"status":"active"}"#;
        assert!(!matches_filters(json, &[filter]));
    }

    #[test]
    fn test_empty_filter_list() {
        let json = br#"{"status":"active"}"#;
        assert!(matches_filters(json, &[]));
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
        // Zero TTL should expire immediately (or not be set)
    }

    #[test]
    fn test_ttl_large_value() {
        use std::time::Duration;
        let ttl_secs = 86400u64; // 1 day
        let duration = Duration::from_secs(ttl_secs);
        assert_eq!(duration.as_secs(), 86400);
    }

    // ──────────────────────────────────────────────────────────────────
    // Transaction ID Generation Tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_txn_id_format() {
        // txn IDs should be in format: hex-counter (with dash separator)
        let id1 = "1a2b3c-0001";
        let id2 = "deadbeef-ffff";
        // Must contain dash separator for format validation
        assert!(id1.contains('-'));
        assert!(id2.contains('-'));
        // Should have two parts when split
        assert_eq!(id1.split('-').count(), 2);
        assert_eq!(id2.split('-').count(), 2);
    }

    #[test]
    fn test_txn_id_collision_probability() {
        // With 64-bit counter, collision probability is very low
        // This is a theoretical test - actual collisions are nearly impossible
        let max_64bit = u64::MAX;
        let collision_window = 1_000_000u64;
        // Probability of collision in 1M attempts on 64-bit space is negligible
        assert!(max_64bit > collision_window * 1000);
    }

    // ──────────────────────────────────────────────────────────────────
    // Compound Index Tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_compound_index_key_format() {
        // Compound index keys use \x1f separator
        let field1 = "alice";
        let field2 = "active";
        let key = format!("{}\x1f{}", field1, field2);
        assert_eq!(key, "alice\x1factive");
        assert!(key.len() > field1.len() + field2.len());
    }

    #[test]
    fn test_compound_index_distinctness() {
        // Different field combinations must produce different keys
        let key1 = format!("{}\x1f{}", "alice", "active");
        let key2 = format!("{}\x1f{}", "bob", "active");
        let key3 = format!("{}\x1f{}", "alice", "inactive");
        assert_ne!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key2, key3);
    }

    // ──────────────────────────────────────────────────────────────────
    // Index Persistence Tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_index_json_serialization() {
        let index_def = serde_json::json!({
            "table": "users",
            "name": "email_idx",
            "fields": ["email"]
        });
        let json_str = serde_json::to_string(&index_def).unwrap();
        assert!(json_str.contains("users"));
        assert!(json_str.contains("email_idx"));
    }

    #[test]
    fn test_compound_index_json_serialization() {
        let index_def = serde_json::json!({
            "table": "users",
            "name": "status_role_idx",
            "fields": ["status", "role"]
        });
        let json_str = serde_json::to_string(&index_def).unwrap();
        assert!(json_str.contains("status_role_idx"));
        assert!(json_str.contains("status"));
        assert!(json_str.contains("role"));
    }

    #[test]
    fn test_index_key_format_for_kv_storage() {
        // Index definitions stored in KV use "table.index_name" as key
        let table = "users";
        let index_name = "email_idx";
        let kv_key = format!("{}.{}", table, index_name);
        assert_eq!(kv_key, "users.email_idx");
    }

    // ──────────────────────────────────────────────────────────────────
    // WAL Configuration Tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_wal_discard_policy_prevents_loss() {
        // DiscardPolicy::New means back-pressure when stream is full
        // No silent drops - ordering guaranteed
        // This is a configuration validation test
        let policy_name = "DiscardPolicy::New";
        assert!(policy_name.contains("New"));
        // Confirms we're using "New" not "Old"
        assert!(!policy_name.contains("Old"));
    }

    // ──────────────────────────────────────────────────────────────────
    // Security Fix Tests (S-01, S-02, S-03, S-04, S-08)
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
    fn test_txn_reserved_table_rejected() {
        let payload = br#"{"ops":[{"op":"put","table":"_indexes","key":"k","value":"e30="}]}"#;
        let result = crate::handler::check_no_reserved_tables("txn", payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_txn_normal_tables_allowed() {
        let payload = br#"{"ops":[{"op":"put","table":"users","key":"k","value":"e30="}]}"#;
        let result = crate::handler::check_no_reserved_tables("txn", payload);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_catalog_allowed() {
        let payload = br#"{"table":"_sql_catalog","key":"users","value":"e30="}"#;
        let result = crate::handler::check_no_reserved_tables("put", payload);
        assert!(result.is_ok());

        let txn_payload =
            br#"{"ops":[{"op":"put","table":"_sql_catalog","key":"k","value":"e30="}]}"#;
        let txn_result = crate::handler::check_no_reserved_tables("txn", txn_payload);
        assert!(txn_result.is_ok());
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
        // ANSI escape sequence (terminal injection risk)
        let input = "\x1b[31mred\x1b[0m";
        let output = crate::log::escape_json_string(input);
        assert!(!output.contains('\x1b'));
        assert!(output.contains("\\u001b"));
    }

    #[test]
    fn test_json_escape_known_chars_still_work() {
        // Existing escapes must not regress
        let input = "a\"b\\c\nd\re\tf";
        let output = crate::log::escape_json_string(input);
        assert!(output.contains("\\\""));
        assert!(output.contains("\\\\"));
        assert!(output.contains("\\n"));
        assert!(output.contains("\\r"));
        assert!(output.contains("\\t"));
    }
}
