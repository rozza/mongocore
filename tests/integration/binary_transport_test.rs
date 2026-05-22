//! Integration tests for the binary UDS transport protocol.
//!
//! These tests start a binary transport server on a unique socket path,
//! connect to it, perform handshake, and exercise CRUD operations using
//! the binary frame protocol directly.

use std::time::Duration;

use bytes::Bytes;
use bson::doc;
use tokio::net::UnixStream;
use uuid::Uuid;

use mongocore::connection::pool::ConnectionPool;
use mongocore::operations::Operations;
use mongocore::transport::frame::*;
use mongocore::transport::start_binary_transport;

#[path = "../harness/mod.rs"]
mod harness;

const TEST_DB: &str = harness::TEST_DB;

fn unique_collection() -> String {
    format!(
        "test_bintrans_{}",
        Uuid::new_v4().to_string().replace('-', "")
    )
}

fn test_socket_path() -> String {
    format!("/tmp/mongocore_bintest_{}.sock", Uuid::new_v4())
}

/// Build a frame with msg_len computed from envelope + raw_docs.
fn build_frame(opcode: Opcode, req_id: u32, envelope: Vec<u8>, raw_docs: Vec<u8>) -> Frame {
    let msg_len = 2 + 4 + envelope.len() as u32 + raw_docs.len() as u32;
    Frame {
        header: FrameHeader {
            msg_len,
            flags: Flags {
                opcode,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id,
        },
        envelope: Bytes::from(envelope),
        raw_docs: Bytes::from(raw_docs),
    }
}

/// Build a frame with no_reply set.
fn build_fire_and_forget_frame(
    opcode: Opcode,
    req_id: u32,
    envelope: Vec<u8>,
    raw_docs: Vec<u8>,
) -> Frame {
    let msg_len = 2 + 4 + envelope.len() as u32 + raw_docs.len() as u32;
    Frame {
        header: FrameHeader {
            msg_len,
            flags: Flags {
                opcode,
                end_of_stream: false,
                no_reply: true,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id,
        },
        envelope: Bytes::from(envelope),
        raw_docs: Bytes::from(raw_docs),
    }
}

/// Start a binary transport server on the given socket path and return the pool.
async fn start_test_server(socket_path: &str) -> (ConnectionPool, Operations) {
    let pool = harness::get_test_pool().await;
    let operations = Operations::new(pool.clone());

    let mut config = mongocore::config::Config {
        connection_uri: String::new(),
        grpc_port: 0,
        mcp_port: 0,
        llm_api_key: None,
        llm_provider_name: None,
        voyage_api_key: None,
        llm_gateway: None,
        compiled_cache_sync: false,
        log_level: "warn".to_string(),
        multi_tenant_enabled: false,
        tenants: vec![],
        analytics_enabled: false,
        analytics_buffer_size: 10000,
        analytics_flush_interval_secs: 300,
        ingestion: Default::default(),
        grpc_max_message_size: 64 * 1024 * 1024,
        transport: "both".to_string(),
        socket_path: "/tmp/unused.sock".to_string(),
        socket_permissions: 0o600,
        otel_enabled: false,
        otel_endpoint: "http://localhost:4317".to_string(),
        otel_service_name: "mongocore".to_string(),
        stream_batch_size: 1000,
        stream_idle_timeout_secs: 60,
        grpc_compression: "none".to_string(),
        pipeline_timeout_secs: 30,
        pipeline_max_concurrency: 20,
        web_ui_enabled: false,
        web_ui_port: 0,
        binary_socket_path: socket_path.to_string(),
        binary_socket_permissions: 0o600,
        binary_transport_enabled: true,
        binary_max_frame_size: 64 * 1024 * 1024,
        binary_max_concurrent: 64,
    };
    config.binary_socket_path = socket_path.to_string();

    let _handle = start_binary_transport(&config, pool.clone(), operations.clone());

    // Give the server time to bind
    tokio::time::sleep(Duration::from_millis(200)).await;

    (pool, operations)
}

/// Connect to the binary transport socket and perform handshake.
async fn connect_and_handshake(socket_path: &str) -> UnixStream {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .expect("binary transport socket not available");

    // Build handshake envelope
    let env = bson::to_vec(&doc! {
        "client_language": "rust-test",
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let frame = build_frame(Opcode::Handshake, 0, env, Vec::new());
    write_frame(&mut stream, &frame).await.unwrap();

    let resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT)
        .await
        .unwrap();
    assert_eq!(resp.header.flags.opcode, Opcode::Handshake);
    assert!(resp.header.flags.end_of_stream);

    // Verify handshake response envelope has max_frame_size
    let resp_doc: bson::Document = bson::from_slice(&resp.envelope).unwrap();
    assert!(resp_doc.get_i32("max_frame_size").unwrap() > 0);
    assert!(resp_doc.get_i32("max_concurrent").unwrap() > 0);

    stream
}

#[tokio::test]
async fn test_binary_transport_handshake() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;

    let _stream = connect_and_handshake(&socket_path).await;

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_insert_and_find_one() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let unique_id = Uuid::new_v4().to_string();

    // Insert a document
    let insert_doc = doc! { "_id": &unique_id, "name": "binary_test", "value": 42_i32 };
    let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();

    let insert_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "doc_bytes_len": raw_doc_bytes.len() as i32,
    })
    .unwrap();

    let insert_frame = build_frame(Opcode::InsertOne, 1, insert_env, raw_doc_bytes);
    write_frame(&mut stream, &insert_frame).await.unwrap();

    let insert_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT)
        .await
        .unwrap();
    assert_eq!(insert_resp.header.flags.opcode, Opcode::InsertOne);
    assert_eq!(insert_resp.header.req_id, 1);

    let insert_resp_doc: bson::Document = bson::from_slice(&insert_resp.envelope).unwrap();
    let inserted_id = insert_resp_doc.get_str("inserted_id").unwrap();
    assert!(!inserted_id.is_empty());

    // FindOne with filter on _id
    let find_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "_id": &unique_id },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let find_frame = build_frame(Opcode::FindOne, 2, find_env, Vec::new());
    write_frame(&mut stream, &find_frame).await.unwrap();

    let find_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT)
        .await
        .unwrap();
    assert_eq!(find_resp.header.flags.opcode, Opcode::FindOne);
    assert_eq!(find_resp.header.req_id, 2);

    let find_resp_doc: bson::Document = bson::from_slice(&find_resp.envelope).unwrap();
    assert_eq!(find_resp_doc.get_bool("ok"), Ok(true));

    // Parse the raw doc from response
    let doc_bytes_len = find_resp_doc.get_i32("doc_bytes_len").unwrap() as usize;
    assert!(doc_bytes_len > 0);
    let found_doc: bson::Document = bson::from_slice(&find_resp.raw_docs).unwrap();
    assert_eq!(found_doc.get_str("name").unwrap(), "binary_test");
    assert_eq!(found_doc.get_i32("value").unwrap(), 42);

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_fire_and_forget() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let unique_id = Uuid::new_v4().to_string();

    // Insert with no_reply=true (fire and forget)
    let insert_doc = doc! { "_id": &unique_id, "name": "fire_forget", "value": 99_i32 };
    let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();

    let insert_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "doc_bytes_len": raw_doc_bytes.len() as i32,
    })
    .unwrap();

    let insert_frame = build_fire_and_forget_frame(Opcode::InsertOne, 10, insert_env, raw_doc_bytes);
    write_frame(&mut stream, &insert_frame).await.unwrap();

    // No response should come back for fire-and-forget.
    // Send a follow-up FindOne which DOES get a response to prove connection is alive.
    // Small delay to ensure the insert is processed before FindOne.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let find_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "_id": &unique_id },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let find_frame = build_frame(Opcode::FindOne, 11, find_env, Vec::new());
    write_frame(&mut stream, &find_frame).await.unwrap();

    let find_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT)
        .await
        .unwrap();
    // The response should be for req_id 11 (the FindOne), not 10 (the fire-and-forget insert)
    assert_eq!(find_resp.header.req_id, 11);
    assert_eq!(find_resp.header.flags.opcode, Opcode::FindOne);

    // Verify the fire-and-forget insert actually worked
    let find_resp_doc: bson::Document = bson::from_slice(&find_resp.envelope).unwrap();
    assert_eq!(find_resp_doc.get_bool("ok"), Ok(true));
    let doc_bytes_len = find_resp_doc.get_i32("doc_bytes_len").unwrap() as usize;
    assert!(doc_bytes_len > 0, "fire-and-forget insert should have persisted the document");

    let found_doc: bson::Document = bson::from_slice(&find_resp.raw_docs).unwrap();
    assert_eq!(found_doc.get_str("name").unwrap(), "fire_forget");

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_delete() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let unique_id = Uuid::new_v4().to_string();

    // Insert a document
    let insert_doc = doc! { "_id": &unique_id, "name": "to_delete", "value": 1_i32 };
    let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();

    let insert_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "doc_bytes_len": raw_doc_bytes.len() as i32,
    })
    .unwrap();

    let insert_frame = build_frame(Opcode::InsertOne, 20, insert_env, raw_doc_bytes);
    write_frame(&mut stream, &insert_frame).await.unwrap();

    let insert_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT)
        .await
        .unwrap();
    assert_eq!(insert_resp.header.req_id, 20);

    // Delete the document
    let delete_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "_id": &unique_id },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let delete_frame = build_frame(Opcode::DeleteOne, 21, delete_env, Vec::new());
    write_frame(&mut stream, &delete_frame).await.unwrap();

    let delete_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT)
        .await
        .unwrap();
    assert_eq!(delete_resp.header.req_id, 21);
    assert_eq!(delete_resp.header.flags.opcode, Opcode::DeleteOne);

    let delete_resp_doc: bson::Document = bson::from_slice(&delete_resp.envelope).unwrap();
    assert_eq!(delete_resp_doc.get_i64("deleted_count").unwrap(), 1);

    // Try to find it — should get None/not found
    let find_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "_id": &unique_id },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let find_frame = build_frame(Opcode::FindOne, 22, find_env, Vec::new());
    write_frame(&mut stream, &find_frame).await.unwrap();

    let find_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT)
        .await
        .unwrap();
    assert_eq!(find_resp.header.req_id, 22);

    let find_resp_doc: bson::Document = bson::from_slice(&find_resp.envelope).unwrap();
    let doc_bytes_len = find_resp_doc.get_i32("doc_bytes_len").unwrap();
    assert_eq!(doc_bytes_len, 0, "deleted document should not be found");

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_update_one() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let unique_id = Uuid::new_v4().to_string();

    // Insert a document
    let insert_doc = doc! { "_id": &unique_id, "name": "original", "value": 10_i32 };
    let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();

    let insert_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "doc_bytes_len": raw_doc_bytes.len() as i32,
    })
    .unwrap();

    let insert_frame = build_frame(Opcode::InsertOne, 30, insert_env, raw_doc_bytes);
    write_frame(&mut stream, &insert_frame).await.unwrap();
    let _insert_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();

    // Update the document
    let update_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "_id": &unique_id },
        "update": { "$set": { "name": "updated", "value": 99_i32 } },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let update_frame = build_frame(Opcode::UpdateOne, 31, update_env, Vec::new());
    write_frame(&mut stream, &update_frame).await.unwrap();

    let update_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(update_resp.header.req_id, 31);
    assert_eq!(update_resp.header.flags.opcode, Opcode::UpdateOne);

    let update_resp_doc: bson::Document = bson::from_slice(&update_resp.envelope).unwrap();
    assert_eq!(update_resp_doc.get_i64("matched_count").unwrap(), 1);
    assert_eq!(update_resp_doc.get_i64("modified_count").unwrap(), 1);

    // Verify the update via FindOne
    let find_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "_id": &unique_id },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let find_frame = build_frame(Opcode::FindOne, 32, find_env, Vec::new());
    write_frame(&mut stream, &find_frame).await.unwrap();

    let find_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    let found_doc: bson::Document = bson::from_slice(&find_resp.raw_docs).unwrap();
    assert_eq!(found_doc.get_str("name").unwrap(), "updated");
    assert_eq!(found_doc.get_i32("value").unwrap(), 99);

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_update_many() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let tag = Uuid::new_v4().to_string();

    // Insert 3 documents with the same tag
    for i in 0..3_i32 {
        let insert_doc = doc! { "tag": &tag, "value": i, "status": "pending" };
        let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();
        let insert_env = bson::to_vec(&doc! {
            "db": TEST_DB,
            "coll": &coll,
            "doc_bytes_len": raw_doc_bytes.len() as i32,
        })
        .unwrap();

        let insert_frame = build_frame(Opcode::InsertOne, 40 + i as u32, insert_env, raw_doc_bytes);
        write_frame(&mut stream, &insert_frame).await.unwrap();
        let _resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    }

    // UpdateMany: set status to "done" for all docs with this tag
    let update_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "tag": &tag },
        "update": { "$set": { "status": "done" } },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let update_frame = build_frame(Opcode::UpdateMany, 50, update_env, Vec::new());
    write_frame(&mut stream, &update_frame).await.unwrap();

    let update_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(update_resp.header.req_id, 50);
    assert_eq!(update_resp.header.flags.opcode, Opcode::UpdateMany);

    let update_resp_doc: bson::Document = bson::from_slice(&update_resp.envelope).unwrap();
    assert_eq!(update_resp_doc.get_i64("matched_count").unwrap(), 3);
    assert_eq!(update_resp_doc.get_i64("modified_count").unwrap(), 3);

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_delete_many() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let tag = Uuid::new_v4().to_string();

    // Insert 3 documents
    for i in 0..3_i32 {
        let insert_doc = doc! { "tag": &tag, "value": i };
        let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();
        let insert_env = bson::to_vec(&doc! {
            "db": TEST_DB,
            "coll": &coll,
            "doc_bytes_len": raw_doc_bytes.len() as i32,
        })
        .unwrap();

        let insert_frame = build_frame(Opcode::InsertOne, 60 + i as u32, insert_env, raw_doc_bytes);
        write_frame(&mut stream, &insert_frame).await.unwrap();
        let _resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    }

    // DeleteMany: delete all docs with this tag
    let delete_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "tag": &tag },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let delete_frame = build_frame(Opcode::DeleteMany, 70, delete_env, Vec::new());
    write_frame(&mut stream, &delete_frame).await.unwrap();

    let delete_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(delete_resp.header.req_id, 70);
    assert_eq!(delete_resp.header.flags.opcode, Opcode::DeleteMany);

    let delete_resp_doc: bson::Document = bson::from_slice(&delete_resp.envelope).unwrap();
    assert_eq!(delete_resp_doc.get_i64("deleted_count").unwrap(), 3);

    // Verify count is 0
    let count_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "tag": &tag },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let count_frame = build_frame(Opcode::CountDocuments, 71, count_env, Vec::new());
    write_frame(&mut stream, &count_frame).await.unwrap();

    let count_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    let count_resp_doc: bson::Document = bson::from_slice(&count_resp.envelope).unwrap();
    assert_eq!(count_resp_doc.get_i64("count").unwrap(), 0);

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_count_documents() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let tag = Uuid::new_v4().to_string();

    // Insert 4 documents, 3 with status "active"
    for i in 0..4_i32 {
        let status = if i < 3 { "active" } else { "inactive" };
        let insert_doc = doc! { "tag": &tag, "value": i, "status": status };
        let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();
        let insert_env = bson::to_vec(&doc! {
            "db": TEST_DB,
            "coll": &coll,
            "doc_bytes_len": raw_doc_bytes.len() as i32,
        })
        .unwrap();

        let insert_frame = build_frame(Opcode::InsertOne, 80 + i as u32, insert_env, raw_doc_bytes);
        write_frame(&mut stream, &insert_frame).await.unwrap();
        let _resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    }

    // Count documents with filter status="active"
    let count_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "tag": &tag, "status": "active" },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let count_frame = build_frame(Opcode::CountDocuments, 90, count_env, Vec::new());
    write_frame(&mut stream, &count_frame).await.unwrap();

    let count_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(count_resp.header.req_id, 90);
    assert_eq!(count_resp.header.flags.opcode, Opcode::CountDocuments);

    let count_resp_doc: bson::Document = bson::from_slice(&count_resp.envelope).unwrap();
    assert_eq!(count_resp_doc.get_i64("count").unwrap(), 3);

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_insert_many() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let tag = Uuid::new_v4().to_string();

    // Build 5 documents as concatenated BSON bytes
    let mut raw_docs_bytes = Vec::new();
    for i in 0..5_i32 {
        let d = doc! { "tag": &tag, "index": i };
        let bytes = bson::to_vec(&d).unwrap();
        raw_docs_bytes.extend_from_slice(&bytes);
    }

    let insert_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "ordered": true,
        "doc_bytes_len": raw_docs_bytes.len() as i32,
    })
    .unwrap();

    let insert_frame = build_frame(Opcode::InsertMany, 100, insert_env, raw_docs_bytes);
    write_frame(&mut stream, &insert_frame).await.unwrap();

    let insert_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(insert_resp.header.req_id, 100);
    assert_eq!(insert_resp.header.flags.opcode, Opcode::InsertMany);

    let insert_resp_doc: bson::Document = bson::from_slice(&insert_resp.envelope).unwrap();
    assert_eq!(insert_resp_doc.get_i32("inserted_count").unwrap(), 5);

    // Verify count
    let count_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": { "tag": &tag },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let count_frame = build_frame(Opcode::CountDocuments, 101, count_env, Vec::new());
    write_frame(&mut stream, &count_frame).await.unwrap();

    let count_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    let count_resp_doc: bson::Document = bson::from_slice(&count_resp.envelope).unwrap();
    assert_eq!(count_resp_doc.get_i64("count").unwrap(), 5);

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_aggregate() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();
    let tag = Uuid::new_v4().to_string();

    // Insert documents with categories
    let docs_data = vec![
        ("catA", 10_i32),
        ("catA", 20),
        ("catB", 30),
        ("catB", 40),
        ("catB", 50),
    ];

    for (i, (category, value)) in docs_data.iter().enumerate() {
        let insert_doc = doc! { "tag": &tag, "category": *category, "value": *value };
        let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();
        let insert_env = bson::to_vec(&doc! {
            "db": TEST_DB,
            "coll": &coll,
            "doc_bytes_len": raw_doc_bytes.len() as i32,
        })
        .unwrap();

        let insert_frame = build_frame(Opcode::InsertOne, 110 + i as u32, insert_env, raw_doc_bytes);
        write_frame(&mut stream, &insert_frame).await.unwrap();
        let _resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    }

    // Aggregate: $match on tag, $group by category with sum of value
    let pipeline = vec![
        doc! { "$match": { "tag": &tag } },
        doc! { "$group": { "_id": "$category", "total": { "$sum": "$value" } } },
        doc! { "$sort": { "_id": 1_i32 } },
    ];
    let pipeline_bson: Vec<bson::Bson> = pipeline.into_iter().map(bson::Bson::Document).collect();

    let agg_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "pipeline": pipeline_bson,
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let agg_frame = build_frame(Opcode::Aggregate, 120, agg_env, Vec::new());
    write_frame(&mut stream, &agg_frame).await.unwrap();

    let agg_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(agg_resp.header.req_id, 120);
    assert_eq!(agg_resp.header.flags.opcode, Opcode::Aggregate);

    let agg_resp_doc: bson::Document = bson::from_slice(&agg_resp.envelope).unwrap();
    let count = agg_resp_doc.get_i32("count").unwrap();
    assert_eq!(count, 2); // catA and catB groups

    // Parse the batch results from raw_docs
    let doc_bytes_len = agg_resp_doc.get_i32("doc_bytes_len").unwrap() as usize;
    assert!(doc_bytes_len > 0);

    // Parse concatenated BSON docs from raw_docs
    let mut results = Vec::new();
    let raw = &agg_resp.raw_docs[..];
    let mut offset = 0;
    while offset < raw.len() {
        let len = u32::from_le_bytes([raw[offset], raw[offset + 1], raw[offset + 2], raw[offset + 3]]) as usize;
        let result_doc: bson::Document = bson::from_slice(&raw[offset..offset + len]).unwrap();
        results.push(result_doc);
        offset += len;
    }

    assert_eq!(results.len(), 2);
    // Sorted by _id, catA comes first
    assert_eq!(results[0].get_str("_id").unwrap(), "catA");
    assert_eq!(results[0].get_i32("total").unwrap(), 30); // 10+20
    assert_eq!(results[1].get_str("_id").unwrap(), "catB");
    assert_eq!(results[1].get_i32("total").unwrap(), 120); // 30+40+50

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_run_command() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    // Send a {ping: 1} command
    let cmd_env = bson::to_vec(&doc! {
        "db": "admin",
        "command": { "ping": 1_i32 },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let cmd_frame = build_frame(Opcode::RunCommand, 130, cmd_env, Vec::new());
    write_frame(&mut stream, &cmd_frame).await.unwrap();

    let cmd_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(cmd_resp.header.req_id, 130);
    assert_eq!(cmd_resp.header.flags.opcode, Opcode::RunCommand);

    // CommandResult has doc_bytes_len in envelope and the result doc in raw_docs
    let cmd_resp_env: bson::Document = bson::from_slice(&cmd_resp.envelope).unwrap();
    let doc_bytes_len = cmd_resp_env.get_i32("doc_bytes_len").unwrap() as usize;
    assert!(doc_bytes_len > 0);

    let result_doc: bson::Document = bson::from_slice(&cmd_resp.raw_docs).unwrap();
    // ping should return ok: 1.0
    let ok_val = result_doc.get_f64("ok").unwrap();
    assert_eq!(ok_val, 1.0);

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_create_and_drop_collection() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll_name = unique_collection();

    // Create collection via RunCommand
    let create_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "command": { "create": &coll_name },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let create_frame = build_frame(Opcode::RunCommand, 140, create_env, Vec::new());
    write_frame(&mut stream, &create_frame).await.unwrap();

    let create_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(create_resp.header.req_id, 140);
    assert_eq!(create_resp.header.flags.opcode, Opcode::RunCommand);

    let create_resp_env: bson::Document = bson::from_slice(&create_resp.envelope).unwrap();
    let doc_bytes_len = create_resp_env.get_i32("doc_bytes_len").unwrap() as usize;
    assert!(doc_bytes_len > 0);

    let create_result: bson::Document = bson::from_slice(&create_resp.raw_docs).unwrap();
    assert_eq!(create_result.get_f64("ok").unwrap(), 1.0);

    // Verify collection exists via ListCollections
    let list_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let list_frame = build_frame(Opcode::ListCollections, 141, list_env, Vec::new());
    write_frame(&mut stream, &list_frame).await.unwrap();

    let list_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(list_resp.header.req_id, 141);
    assert_eq!(list_resp.header.flags.opcode, Opcode::ListCollections);

    let list_resp_doc: bson::Document = bson::from_slice(&list_resp.envelope).unwrap();
    let names = list_resp_doc.get_array("names").unwrap();
    let name_strings: Vec<&str> = names.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        name_strings.contains(&coll_name.as_str()),
        "created collection should appear in list"
    );

    // Drop collection via RunCommand
    let drop_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "command": { "drop": &coll_name },
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let drop_frame = build_frame(Opcode::RunCommand, 142, drop_env, Vec::new());
    write_frame(&mut stream, &drop_frame).await.unwrap();

    let drop_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(drop_resp.header.req_id, 142);

    // Verify dropped - list should no longer contain it
    let list_env2 = bson::to_vec(&doc! {
        "db": TEST_DB,
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let list_frame2 = build_frame(Opcode::ListCollections, 143, list_env2, Vec::new());
    write_frame(&mut stream, &list_frame2).await.unwrap();

    let list_resp2 = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    let list_resp_doc2: bson::Document = bson::from_slice(&list_resp2.envelope).unwrap();
    let names2 = list_resp_doc2.get_array("names").unwrap();
    let name_strings2: Vec<&str> = names2.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        !name_strings2.contains(&coll_name.as_str()),
        "dropped collection should not appear in list"
    );

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_list_collections() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();

    // Insert a document to implicitly create the collection
    let insert_doc = doc! { "test": true };
    let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();
    let insert_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "doc_bytes_len": raw_doc_bytes.len() as i32,
    })
    .unwrap();

    let insert_frame = build_frame(Opcode::InsertOne, 150, insert_env, raw_doc_bytes);
    write_frame(&mut stream, &insert_frame).await.unwrap();
    let _resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();

    // List collections
    let list_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let list_frame = build_frame(Opcode::ListCollections, 151, list_env, Vec::new());
    write_frame(&mut stream, &list_frame).await.unwrap();

    let list_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(list_resp.header.req_id, 151);
    assert_eq!(list_resp.header.flags.opcode, Opcode::ListCollections);

    let list_resp_doc: bson::Document = bson::from_slice(&list_resp.envelope).unwrap();
    let names = list_resp_doc.get_array("names").unwrap();
    let name_strings: Vec<&str> = names.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        name_strings.contains(&coll.as_str()),
        "newly created collection should appear in list_collections"
    );

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}

#[tokio::test]
async fn test_binary_transport_batch_follows() {
    let socket_path = test_socket_path();
    let (_pool, _ops) = start_test_server(&socket_path).await;
    let mut stream = connect_and_handshake(&socket_path).await;

    let coll = unique_collection();

    // Send 3 insert requests: first 2 with batch_follows=true, last without
    let mut req_ids = Vec::new();
    for i in 0..3_u32 {
        let insert_doc = doc! { "batch_item": i as i32 };
        let raw_doc_bytes = bson::to_vec(&insert_doc).unwrap();
        let insert_env = bson::to_vec(&doc! {
            "db": TEST_DB,
            "coll": &coll,
            "doc_bytes_len": raw_doc_bytes.len() as i32,
        })
        .unwrap();

        let batch_follows = i < 2; // first 2 have batch_follows=true
        let msg_len = 2 + 4 + insert_env.len() as u32 + raw_doc_bytes.len() as u32;
        let frame = Frame {
            header: FrameHeader {
                msg_len,
                flags: Flags {
                    opcode: Opcode::InsertOne,
                    end_of_stream: false,
                    no_reply: false,
                    batch_follows,
                    priority: Priority::Normal,
                },
                req_id: 160 + i,
            },
            envelope: Bytes::from(insert_env),
            raw_docs: Bytes::from(raw_doc_bytes),
        };

        write_frame(&mut stream, &frame).await.unwrap();
        req_ids.push(160 + i);
    }

    // All 3 responses should come back (flushed together after last frame)
    for expected_id in &req_ids {
        let resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
        assert_eq!(resp.header.req_id, *expected_id);
        assert_eq!(resp.header.flags.opcode, Opcode::InsertOne);

        let resp_doc: bson::Document = bson::from_slice(&resp.envelope).unwrap();
        let inserted_id = resp_doc.get_str("inserted_id").unwrap();
        assert!(!inserted_id.is_empty());
    }

    // Verify all 3 docs were inserted
    let count_env = bson::to_vec(&doc! {
        "db": TEST_DB,
        "coll": &coll,
        "filter": {},
        "doc_bytes_len": 0_i32,
    })
    .unwrap();

    let count_frame = build_frame(Opcode::CountDocuments, 170, count_env, Vec::new());
    write_frame(&mut stream, &count_frame).await.unwrap();

    let count_resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    let count_resp_doc: bson::Document = bson::from_slice(&count_resp.envelope).unwrap();
    assert_eq!(count_resp_doc.get_i64("count").unwrap(), 3);

    // Cleanup
    let _ = std::fs::remove_file(&socket_path);
}
