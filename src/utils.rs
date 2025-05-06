use rand::Rng;
use rand::distr::Alphanumeric;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    if s.len() < 2 {
        return Err("Duration string too short".to_string());
    }

    if s.ends_with("ms") {
        let num: u64 = s[..s.len() - 2]
            .parse()
            .map_err(|_| "Invalid duration number".to_string())?;
        return Ok(Duration::from_millis(num));
    }

    let (num, unit) = s.split_at(s.len() - 1);
    let num: u64 = num
        .parse()
        .map_err(|_| "Invalid duration number".to_string())?;

    match unit {
        "s" => Ok(Duration::from_secs(num)),
        "m" => Ok(Duration::from_secs(num * 60)),
        "h" => Ok(Duration::from_secs(num * 3600)),
        "d" => Ok(Duration::from_secs(num * 86400)),
        _ => Err("Invalid duration unit".to_string()),
    }
}

pub fn parse_rate(s: &str) -> Result<u64, String> {
    if s.is_empty() {
        return Err("Empty rate string".to_string());
    }

    if s.ends_with('k') {
        let num: u64 = s[..s.len() - 1]
            .parse()
            .map_err(|_| "Invalid rate number".to_string())?;
        Ok(num * 1000)
    } else {
        s.parse().map_err(|_| "Invalid rate number".to_string())
    }
}

pub fn parse_bandwidth(s: &str) -> Result<u64, String> {
    if s.is_empty() {
        return Err("Empty bandwidth string".to_string());
    }

    let s = s.to_lowercase();
    if s.ends_with("kbps") {
        let num: u64 = s[..s.len() - 4]
            .parse()
            .map_err(|_| "Invalid bandwidth number".to_string())?;
        Ok(num * 1000)
    } else if s.ends_with("mbps") {
        let num: u64 = s[..s.len() - 4]
            .parse()
            .map_err(|_| "Invalid bandwidth number".to_string())?;
        Ok(num * 1_000_000)
    } else if s.ends_with("kbps") {
        let num: u64 = s[..s.len() - 3]
            .parse()
            .map_err(|_| "Invalid bandwidth number".to_string())?;
        Ok(num * 1000 * 8)
    } else if s.ends_with("mbps") {
        let num: u64 = s[..s.len() - 3]
            .parse()
            .map_err(|_| "Invalid bandwidth number".to_string())?;
        Ok(num * 1_000_000 * 8)
    } else {
        s.parse()
            .map_err(|_| "Invalid bandwidth number".to_string())
    }
}

pub fn unescape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('0') => result.push('\0'),
                Some('x') => {
                    // Handle hex escape sequences like \x41
                    let hex_digits: String = chars.by_ref().take(2).collect();
                    if hex_digits.len() == 2 {
                        if let Ok(byte) = u8::from_str_radix(&hex_digits, 16) {
                            result.push(byte as char);
                        } else {
                            result.push_str(&format!("\\x{}", hex_digits));
                        }
                    } else {
                        result.push_str("\\x");
                        result.push_str(&hex_digits);
                    }
                }
                Some(c) => {
                    // Unknown escape sequence, keep both characters
                    result.push('\\');
                    result.push(c);
                }
                None => result.push('\\'), // Backslash at end of string
            }
        } else {
            result.push(c);
        }
    }

    result
}

pub fn get_message_arg(
    matches: &clap::ArgMatches,
    arg_name: &str,
    unescape: bool,
) -> Option<Vec<u8>> {
    matches.get_one::<String>(arg_name).map(|s| {
        if unescape {
            unescape_string(s).into_bytes()
        } else {
            s.as_bytes().to_vec()
        }
    })
}

pub fn get_file_arg(matches: &clap::ArgMatches, arg_name: &str, unescape: bool) -> Option<Vec<u8>> {
    matches.get_one::<String>(arg_name).and_then(|filename| {
        match std::fs::read_to_string(filename) {
            Ok(content) => {
                if unescape {
                    Some(unescape_string(&content).into_bytes())
                } else {
                    Some(content.into_bytes())
                }
            }
            Err(e) => {
                eprintln!("Failed to read file {}: {}", filename, e);
                None
            }
        }
    })
}

pub fn generate_payload(size: usize) -> Vec<u8> {
    rand::rng().sample_iter(&Alphanumeric).take(size).collect()
}

pub fn unix_timestamp_millis() -> u64 {
    let now = SystemTime::now();
    let timestamp = now.duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;
    return timestamp;
}
