//! The client's side of a run's workspace, for the tests.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::os::unix::net::UnixStream;

use serde_json::{Value, json};
use tacklr_sandbox_worker::frame::KIND_FS;

/// The client's side of a run's workspace, in memory: files by path, the
/// directories, and the open files by handle.
#[derive(Default)]
pub struct Workspace {
    pub files: BTreeMap<String, Vec<u8>>,
    pub dirs: BTreeSet<String>,
    pub open: HashMap<u64, String>,
    next: u64,
}

impl Workspace {
    /// Answers one file request frame.
    pub fn answer(&mut self, socket: &mut UnixStream, body: &[u8]) {
        let json = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
        let request: Value = serde_json::from_slice(&body[4..4 + json]).expect("file request");
        let data = &body[4 + json..];
        let text = |key: &str| request[key].as_str().unwrap_or_default().to_owned();
        let number =
            |key: &str| usize::try_from(request[key].as_u64().unwrap_or_default()).unwrap();
        let mut reply = json!({"type": "fs_reply", "id": request["id"]});
        let mut bytes = Vec::new();
        let path = text("path");
        match request["op"].as_str().expect("op") {
            "stat" if path == "/" || self.dirs.contains(&path) => {
                reply["is_dir"] = json!(true);
            }
            "stat" if self.files.contains_key(&path) => {
                reply["size"] = json!(self.files[&path].len());
            }
            "stat" => {
                reply["error"] = json!("ENOENT");
            }
            "read_dir" => {
                let prefix = if path == "/" {
                    "/".to_owned()
                } else {
                    format!("{path}/")
                };
                let entries: Vec<Value> = self
                    .files
                    .keys()
                    .map(|file| (file, false))
                    .chain(self.dirs.iter().map(|dir| (dir, true)))
                    .filter_map(|(entry, is_dir)| Some((entry.strip_prefix(&prefix)?, is_dir)))
                    .filter(|(name, _)| !name.contains('/'))
                    .map(|(name, is_dir)| json!({"name": name, "is_dir": is_dir}))
                    .collect();
                reply["entries"] = json!(entries);
            }
            "open"
                if !self.files.contains_key(&path)
                    && !request["create"].as_bool().unwrap_or_default() =>
            {
                reply["error"] = json!("ENOENT");
            }
            "open" => {
                let file = self.files.entry(path.clone()).or_default();
                if request["truncate"].as_bool().unwrap_or_default() {
                    file.clear();
                }
                reply["size"] = json!(file.len());
                self.next += 1;
                self.open.insert(self.next, path);
                reply["handle"] = json!(self.next);
            }
            "read" => {
                let file = &self.files[&self.open[&(number("handle") as u64)]];
                let from = number("offset").min(file.len());
                let to = (from + number("length")).min(file.len());
                bytes = file[from..to].to_vec();
            }
            "write" => {
                let file = self
                    .files
                    .get_mut(&self.open[&(number("handle") as u64)])
                    .expect("open file");
                let offset = number("offset");
                if file.len() < offset + data.len() {
                    file.resize(offset + data.len(), 0);
                }
                file[offset..offset + data.len()].copy_from_slice(data);
                reply["size"] = json!(file.len());
            }
            "truncate" => {
                let file = self
                    .files
                    .get_mut(&self.open[&(number("handle") as u64)])
                    .expect("open file");
                file.resize(number("size"), 0);
            }
            "close" => {
                self.open.remove(&(number("handle") as u64));
            }
            "mkdir" => {
                self.dirs.insert(path);
            }
            "remove" => {
                self.files.remove(&path);
                self.dirs.remove(&path);
            }
            "rename" => {
                let moved = self.files.remove(&text("from")).expect("rename source");
                self.files.insert(text("to"), moved);
            }
            op => panic!("unknown file op {op}"),
        }
        let json = serde_json::to_vec(&reply).expect("encode reply");
        let mut frame = u32::try_from(json.len()).unwrap().to_be_bytes().to_vec();
        frame.extend_from_slice(&json);
        frame.extend_from_slice(&bytes);
        write_raw(socket, u32::try_from(frame.len()).unwrap(), KIND_FS, &frame);
    }
}

fn write_raw(socket: &mut UnixStream, length: u32, kind: u8, body: &[u8]) {
    let mut bytes = length.to_be_bytes().to_vec();
    bytes.push(kind);
    bytes.extend_from_slice(body);
    socket.write_all(&bytes).expect("write raw frame");
}
