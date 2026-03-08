use std::fs;
use std::path::Path;
use std::sync::OnceLock;

const TARGET_INBOX_BUCKET: u8 = 0xFF;
const CLIENT_ID_SUFFIX_ALPHABET: &[u8] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";

pub fn java_string_hash_code(value: &str) -> i32 {
    let mut hash = 0i32;
    for unit in value.encode_utf16() {
        hash = hash.wrapping_mul(31).wrapping_add(unit as i32);
    }
    hash
}

fn java_hash_append_char(hash: i32, ch: u8) -> i32 {
    hash.wrapping_mul(31).wrapping_add(ch as i32)
}

pub fn inbox_bucket(user_id: &str, client_id: &str) -> u8 {
    let inbox_id = format!("{user_id}/{client_id}");
    let hash = java_string_hash_code(&inbox_id) as u32;
    ((hash ^ (hash >> 16)) & 0xFF) as u8
}

fn suffix_tail_options() -> &'static [(i32, [u8; 2])] {
    static OPTIONS: OnceLock<Vec<(i32, [u8; 2])>> = OnceLock::new();
    OPTIONS.get_or_init(|| {
        let mut options = Vec::with_capacity(CLIENT_ID_SUFFIX_ALPHABET.len().pow(2));
        for &first in CLIENT_ID_SUFFIX_ALPHABET {
            for &second in CLIENT_ID_SUFFIX_ALPHABET {
                let hash = java_hash_append_char(java_hash_append_char(0, first), second);
                options.push((hash, [first, second]));
            }
        }
        options
    })
}

pub fn generate_bucketed_client_id(user_id: &str, node_id: &str, index: usize) -> String {
    let base = format!("{}_{}", node_id, index);
    let prefix_hash = java_string_hash_code(&format!("{user_id}/{base}"));
    let tail_options = suffix_tail_options();
    let factor_31_squared = 31i32.wrapping_mul(31);
    for &head in CLIENT_ID_SUFFIX_ALPHABET {
        let hash_with_head = java_hash_append_char(prefix_hash, head);
        let base_hash = hash_with_head.wrapping_mul(factor_31_squared);
        for &(tail_hash, tail) in tail_options {
            let final_hash = base_hash.wrapping_add(tail_hash);
            let hash = final_hash as u32;
            let bucket = ((hash ^ (hash >> 16)) & 0xFF) as u8;
            if bucket == TARGET_INBOX_BUCKET {
                let mut client_id = String::with_capacity(base.len() + 3);
                client_id.push_str(&base);
                client_id.push(head as char);
                client_id.push(tail[0] as char);
                client_id.push(tail[1] as char);
                return client_id;
            }
        }
    }
    unreachable!("suffix search must find a client id")
}

pub fn generate_bucketed_client_ids(user_id: &str, node_id: &str, client_count: usize) -> Vec<String> {
    (0..client_count)
        .map(|index| generate_bucketed_client_id(user_id, node_id, index))
        .collect()
}

pub fn persist_client_ids(path: &str, client_ids: &[String]) -> Result<(), String> {
    if client_ids.is_empty() {
        return Ok(());
    }
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
    }
    fs::write(path_ref, format!("{}\n", client_ids.join("\n"))).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_client_id_hits_target_bucket() {
        let user_id = "user-a";
        let client_id = generate_bucketed_client_id(user_id, "node-1", 0);
        assert_eq!(inbox_bucket(user_id, &client_id), TARGET_INBOX_BUCKET);
    }
}
