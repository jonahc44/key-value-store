//! Minimal HTTP/1.1 client built on Tokio. Raft peers and forwarded client
//! calls use it so the server does not need an extra HTTP client crate.

use serde_json::Value;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const RPC_TIMEOUT: Duration = Duration::from_millis(300);

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn call(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<(u16, Vec<u8>), String> {
    let headers: &[(&str, &str)] = if body.is_some() {
        &[("content-type", "application/json")]
    } else {
        &[]
    };
    tokio::time::timeout(
        Duration::from_secs(3),
        request(addr, method, path, body.as_deref(), headers),
    )
    .await
    .map_err(|_| "timeout".to_string())?
}

pub async fn post_json(addr: &str, path: &str, body: &Value) -> Result<Value, String> {
    let bytes = serde_json::to_vec(body).map_err(|err| err.to_string())?;
    let result = async {
        let (status, response) = tokio::time::timeout(
            RPC_TIMEOUT,
            request(
                addr,
                "POST",
                path,
                Some(&bytes),
                &[("content-type", "application/json")],
            ),
        )
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|err| err)?;
        if status != 200 {
            return Err(format!("status {status}"));
        }
        serde_json::from_slice(&response).map_err(|err| err.to_string())
    }
    .await;
    if let Err(err) = &result {
        tracing::debug!(%addr, %path, %err, "raft rpc failed");
    }
    result
}

pub(crate) async fn post(
    addr: &str,
    path: &str,
    body: &Value,
    timeout: Duration,
) -> Result<(u16, Vec<u8>), String> {
    let bytes = serde_json::to_vec(body).map_err(|err| err.to_string())?;
    tokio::time::timeout(
        timeout,
        request(
            addr,
            "POST",
            path,
            Some(&bytes),
            &[("content-type", "application/json")],
        ),
    )
    .await
    .map_err(|_| "timeout".to_string())?
}

pub async fn forward(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    timeout: Duration,
) -> Result<(u16, Vec<u8>), String> {
    let extra = [
        ("content-type", "application/json"),
        ("x-raft-forwarded", "1"),
    ];
    tokio::time::timeout(
        timeout,
        request(addr, method, path, body.as_deref(), &extra),
    )
    .await
    .map_err(|_| "timeout".to_string())?
}

async fn request(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    headers: &[(&str, &str)],
) -> Result<(u16, Vec<u8>), String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|err| err.to_string())?;
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    if let Some(body) = body {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    } else {
        head.push_str("Content-Length: 0\r\n");
    }
    head.push_str("\r\n");
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|err| err.to_string())?;
    if let Some(body) = body {
        stream
            .write_all(body)
            .await
            .map_err(|err| err.to_string())?;
    }

    read_response(&mut stream).await
}

async fn read_response(stream: &mut TcpStream) -> Result<(u16, Vec<u8>), String> {
    let mut buf = Vec::new();
    let header_end = loop {
        if let Some(pos) = find_slice(&buf, b"\r\n\r\n") {
            break pos;
        }
        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp).await.map_err(|err| err.to_string())?;
        if n == 0 {
            return Err("connection closed before headers".into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 1024 * 1024 {
            return Err("response headers too large".into());
        }
    };

    let header_text = std::str::from_utf8(&buf[..header_end]).map_err(|err| err.to_string())?;
    let mut lines = header_text.split("\r\n");
    let status_line = lines.next().ok_or("empty response")?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("missing status code")?
        .parse()
        .map_err(|_| format!("bad status line: {status_line}"))?;

    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| "bad content-length")?,
            );
        } else if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
            chunked = true;
        }
    }

    let mut body = buf.split_off(header_end + 4);
    if chunked {
        let decoded = read_chunked(stream, &mut body).await?;
        return Ok((status, decoded));
    }
    if let Some(length) = content_length {
        while body.len() < length {
            let mut tmp = [0u8; 4096];
            let n = stream.read(&mut tmp).await.map_err(|err| err.to_string())?;
            if n == 0 {
                return Err("connection closed before body".into());
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(length);
        return Ok((status, body));
    }

    loop {
        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp).await.map_err(|err| err.to_string())?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    Ok((status, body))
}

async fn read_chunked(stream: &mut TcpStream, pending: &mut Vec<u8>) -> Result<Vec<u8>, String> {
    let mut decoded = Vec::new();
    loop {
        let line_end = loop {
            if let Some(pos) = find_slice(pending, b"\r\n") {
                break pos;
            }
            read_more(stream, pending).await?;
        };
        let line = std::str::from_utf8(&pending[..line_end]).map_err(|err| err.to_string())?;
        let size_text = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_text, 16).map_err(|_| "bad chunk size")?;
        pending.drain(..line_end + 2);
        if size == 0 {
            return Ok(decoded);
        }
        while pending.len() < size + 2 {
            read_more(stream, pending).await?;
        }
        decoded.extend_from_slice(&pending[..size]);
        pending.drain(..size + 2);
    }
}

async fn read_more(stream: &mut TcpStream, pending: &mut Vec<u8>) -> Result<(), String> {
    let mut tmp = [0u8; 4096];
    let n = stream.read(&mut tmp).await.map_err(|err| err.to_string())?;
    if n == 0 {
        return Err("connection closed".into());
    }
    pending.extend_from_slice(&tmp[..n]);
    Ok(())
}

fn find_slice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
