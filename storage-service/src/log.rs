/// Structured JSON logging module.
/// Formats log messages as JSON to stderr for easy parsing and filtering.
use std::fmt;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Level::Info => write!(f, "INFO"),
            Level::Warn => write!(f, "WARN"),
            Level::Error => write!(f, "ERROR"),
        }
    }
}

/// Structured JSON log entry.
/// Fields are added via builder pattern.
#[allow(dead_code)]
pub struct Log {
    level: Level,
    message: String,
    fields: Vec<(String, String)>,
}

#[allow(dead_code)]
impl Log {
    pub fn info(msg: impl Into<String>) -> Self {
        Log {
            level: Level::Info,
            message: msg.into(),
            fields: Vec::new(),
        }
    }

    pub fn warn(msg: impl Into<String>) -> Self {
        Log {
            level: Level::Warn,
            message: msg.into(),
            fields: Vec::new(),
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Log {
            level: Level::Error,
            message: msg.into(),
            fields: Vec::new(),
        }
    }

    /// Add a field to the log entry.
    pub fn field(mut self, key: impl Into<String>, value: impl fmt::Display) -> Self {
        self.fields.push((key.into(), value.to_string()));
        self
    }

    /// Emit the log to stderr as JSON.
    pub fn emit(self) {
        let mut json = format!(
            r#"{{"level":"{}","message":"{}""#,
            self.level,
            escape_json_string(&self.message)
        );

        for (k, v) in self.fields {
            json.push(',');
            json.push_str(&format!(
                r#""{}":"{}""#,
                escape_json_string(&k),
                escape_json_string(&v)
            ));
        }

        json.push('}');
        eprintln!("lattice-db: {json}");
    }
}

/// Escape special characters in JSON strings.
pub fn escape_json_string(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            // S-08: escape remaining control characters to keep JSON valid and
            // prevent terminal escape-sequence injection in log tails.
            c if (c as u32) < 0x20 => result.push_str(&format!("\\u{:04x}", c as u32)),
            _ => result.push(c),
        }
    }
    result
}

/// Convenience macros for common logging patterns.

#[macro_export]
macro_rules! log_info {
    ($msg:expr) => {
        $crate::log::Log::info($msg).emit();
    };
    ($msg:expr, $($key:expr => $val:expr),+) => {
        {
            let mut log = $crate::log::Log::info($msg);
            $(
                log = log.field($key, $val);
            )+
            log.emit();
        }
    };
}

#[macro_export]
macro_rules! log_warn {
    ($msg:expr) => {
        $crate::log::Log::warn($msg).emit();
    };
    ($msg:expr, $($key:expr => $val:expr),+) => {
        {
            let mut log = $crate::log::Log::warn($msg);
            $(
                log = log.field($key, $val);
            )+
            log.emit();
        }
    };
}

#[macro_export]
macro_rules! log_error {
    ($msg:expr) => {
        $crate::log::Log::error($msg).emit();
    };
    ($msg:expr, $($key:expr => $val:expr),+) => {
        {
            let mut log = $crate::log::Log::error($msg);
            $(
                log = log.field($key, $val);
            )+
            log.emit();
        }
    };
}
