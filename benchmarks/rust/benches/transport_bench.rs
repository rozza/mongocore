use criterion::{criterion_group, criterion_main, Criterion};
use bson::doc;
use bytes::Bytes;
use tokio::net::UnixStream;
use tokio::runtime::Runtime;

use mongocore::transport::frame::*;

const BINARY_SOCKET: &str = "/tmp/mongocore.bin.sock";

fn is_server_available() -> bool {
    std::path::Path::new(BINARY_SOCKET).exists()
}

async fn binary_handshake() -> Option<UnixStream> {
    let mut stream = UnixStream::connect(BINARY_SOCKET).await.ok()?;
    let env =
        bson::to_vec(&doc! { "client_language": "bench", "doc_bytes_len": 0_i32 }).unwrap();
    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::Handshake,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 0,
        },
        envelope: Bytes::from(env),
        raw_docs: Bytes::new(),
    };
    write_frame(&mut stream, &frame).await.ok()?;
    read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.ok()?;
    Some(stream)
}

async fn binary_find_one(stream: &mut UnixStream, req_id: u32) {
    let env = bson::to_vec(&doc! {
        "db": "bench_db", "coll": "bench_coll",
        "filter": { "_id": "bench_doc" }, "doc_bytes_len": 0_i32
    })
    .unwrap();
    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::FindOne,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id,
        },
        envelope: Bytes::from(env),
        raw_docs: Bytes::new(),
    };
    write_frame(stream, &frame).await.unwrap();
    let _ = read_frame(stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
}

async fn binary_insert_one(stream: &mut UnixStream, req_id: u32, doc_to_insert: &[u8]) {
    let env = bson::to_vec(&doc! {
        "db": "bench_db", "coll": "bench_coll",
        "doc_bytes_len": doc_to_insert.len() as i32
    })
    .unwrap();
    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::InsertOne,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id,
        },
        envelope: Bytes::from(env),
        raw_docs: Bytes::copy_from_slice(doc_to_insert),
    };
    write_frame(stream, &frame).await.unwrap();
    let _ = read_frame(stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
}

fn bench_find_one(c: &mut Criterion) {
    if !is_server_available() {
        eprintln!("Skipping benchmarks: binary transport socket not available");
        return;
    }
    let rt = Runtime::new().unwrap();
    let mut stream = rt.block_on(binary_handshake()).expect("handshake failed");
    let mut req_id = 1u32;

    c.bench_function("binary_find_one", |b| {
        b.iter(|| {
            rt.block_on(async {
                req_id += 1;
                binary_find_one(&mut stream, req_id).await;
            });
        });
    });
}

fn bench_insert_one(c: &mut Criterion) {
    if !is_server_available() {
        eprintln!("Skipping benchmarks: binary transport socket not available");
        return;
    }
    let rt = Runtime::new().unwrap();
    let mut stream = rt.block_on(binary_handshake()).expect("handshake failed");
    let test_doc = bson::to_vec(&doc! { "x": 1, "data": "benchmark payload" }).unwrap();
    let mut req_id = 1u32;

    c.bench_function("binary_insert_one", |b| {
        b.iter(|| {
            rt.block_on(async {
                req_id += 1;
                binary_insert_one(&mut stream, req_id, &test_doc).await;
            });
        });
    });
}

fn bench_fire_and_forget(c: &mut Criterion) {
    if !is_server_available() {
        eprintln!("Skipping benchmarks: binary transport socket not available");
        return;
    }
    let rt = Runtime::new().unwrap();
    let mut stream = rt.block_on(binary_handshake()).expect("handshake failed");
    let test_doc = bson::to_vec(&doc! { "x": 1, "data": "fire and forget" }).unwrap();
    let mut req_id = 1u32;

    c.bench_function("binary_insert_fire_and_forget", |b| {
        b.iter(|| {
            rt.block_on(async {
                req_id += 1;
                let env = bson::to_vec(&doc! {
                    "db": "bench_db", "coll": "bench_ff",
                    "doc_bytes_len": test_doc.len() as i32
                })
                .unwrap();
                let frame = Frame {
                    header: FrameHeader {
                        msg_len: 0,
                        flags: Flags {
                            opcode: Opcode::InsertOne,
                            end_of_stream: false,
                            no_reply: true,
                            batch_follows: false,
                            priority: Priority::Normal,
                        },
                        req_id,
                    },
                    envelope: Bytes::from(env),
                    raw_docs: Bytes::from(test_doc.clone()),
                };
                write_frame(&mut stream, &frame).await.unwrap();
            });
        });
    });
}

criterion_group!(benches, bench_find_one, bench_insert_one, bench_fire_and_forget);
criterion_main!(benches);
