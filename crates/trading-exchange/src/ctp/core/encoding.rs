//! CTP 定长 `char[]` 字段的编解码。
//!
//! CTP 字符串字段是定长字节数组、以 NUL 截断，中文使用 GB18030 编码。
//! 本模块把它们安全地转换为 Rust UTF-8 `String`，反之亦然。

use encoding_rs::GB18030;

/// 解码定长字节数组：以首个 NUL 为界，按 ASCII/GB18030 解码为 UTF-8 字符串。
///
/// 遇到无法解码的字节时用替换字符兜底，而不是 panic（回报字段不应使流程崩溃）。
pub fn decode_fixed(buf: &[i8]) -> String {
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    // i8 → u8：CTP 的 char 在内存里就是无符号字节。
    let bytes: Vec<u8> = buf[..len].iter().map(|&b| b as u8).collect();

    if bytes.is_ascii() {
        // ASCII 子集必然是合法 UTF-8，直接分配。
        String::from_utf8(bytes).unwrap_or_default()
    } else {
        GB18030.decode(&bytes).0.into_owned()
    }
}

/// 把字符串编码写入定长缓冲：拷贝可容纳部分并保证以 NUL 结尾。
///
/// 注意：这里按 UTF-8 字节截断，多字节中文在边界可能被截成半个字符
/// （调用方应保证缓冲足够容纳合约号等短文本）。
pub fn encode_fixed(buf: &mut [i8], text: &str) {
    if buf.is_empty() {
        return;
    }
    let bytes = text.as_bytes();
    let copy_len = bytes.len().min(buf.len() - 1);
    for (i, &b) in bytes[..copy_len].iter().enumerate() {
        buf[i] = b as i8;
    }
    buf[copy_len] = 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    use encoding_rs::GB18030;

    #[test]
    fn decode_all_zero_buffer_is_empty_string() {
        let buf = [0i8; 16];
        assert_eq!(decode_fixed(&buf), "");
    }

    #[test]
    fn decode_ascii_stops_at_first_nul() {
        // 首个 NUL 之后的脏数据必须被忽略。
        let mut buf = [0i8; 8];
        buf[0] = b'r' as i8;
        buf[1] = b'b' as i8;
        buf[2] = b'2' as i8;
        buf[3] = 0;
        buf[4] = b'X' as i8;
        assert_eq!(decode_fixed(&buf), "rb2");
    }

    #[test]
    fn decode_gb18030_chinese_round_trips() {
        // CTP 的 StatusMsg 等中文文本以 GB18030 编码、NUL 结尾。
        let (bytes, _, _) = GB18030.encode("合约");
        let mut buf = [0i8; 16];
        for (i, b) in bytes.iter().enumerate() {
            buf[i] = *b as i8;
        }
        assert_eq!(decode_fixed(&buf), "合约");
    }

    #[test]
    fn encode_ascii_writes_bytes_and_nul() {
        let mut buf = [0i8; 8];
        encode_fixed(&mut buf, "ab");
        assert_eq!(buf[0], b'a' as i8);
        assert_eq!(buf[1], b'b' as i8);
        assert_eq!(buf[2], 0);
    }

    #[test]
    fn encode_fixed_truncates_to_fit_terminator() {
        // 缓冲区仅容 2 个有效字符 + NUL：超长串被截断而不越界。
        let mut buf = [0i8; 3];
        encode_fixed(&mut buf, "abcd");
        assert_eq!(buf[0], b'a' as i8);
        assert_eq!(buf[1], b'b' as i8);
        assert_eq!(buf[2], 0);
    }

    #[test]
    fn encode_fixed_zero_len_buffer_is_noop() {
        let mut buf = [];
        encode_fixed(&mut buf, "x"); // 不应 panic
    }
}
