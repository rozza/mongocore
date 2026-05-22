//! Opcode dispatch routing for the binary UDS transport protocol.
//!
//! Routes parsed `OperationRequest` variants to the appropriate operations layer
//! methods and converts results into `OperationResponse` variants.

use bson::Document;

use crate::connection::pool::ConnectionPool;
use crate::error::MongoCoreError;
use crate::operations::raw::{run_command, RawCommandOptions};
use crate::operations::{IndexOptions, Operations};
use crate::transport::codec::{OperationRequest, OperationResponse};

/// Handle a handshake request, returning server capabilities.
///
/// If the request is not a Handshake variant, returns an error response.
pub fn handle_handshake(
    req: &OperationRequest,
    max_frame_size: u32,
    max_concurrent: u32,
) -> OperationResponse {
    match req {
        OperationRequest::Handshake { .. } => OperationResponse::Handshake {
            max_frame_size,
            max_concurrent,
        },
        _ => OperationResponse::Error {
            code: 1,
            message: "expected Handshake request".to_string(),
        },
    }
}

/// Dispatch an operation request to the appropriate operations layer method.
///
/// For operations that require document bodies (InsertOne, InsertMany, BulkWrite),
/// the `raw_docs` parameter carries the raw BSON document bytes.
pub async fn dispatch(
    operations: &Operations,
    pool: &ConnectionPool,
    request: OperationRequest,
    raw_docs: &[u8],
) -> OperationResponse {
    match request {
        OperationRequest::Handshake { .. } => OperationResponse::Error {
            code: 1,
            message: "Handshake must be handled via handle_handshake".to_string(),
        },
        OperationRequest::FindOne {
            db,
            collection,
            filter,
            projection: _,
        } => match operations.find_one(&db, &collection, filter).await {
            Ok(doc) => match doc.map(|d| bson::to_vec(&d)).transpose() {
                Ok(raw) => OperationResponse::SingleDoc {
                    ok: true,
                    doc: raw,
                },
                Err(e) => OperationResponse::Error {
                    code: 1,
                    message: format!("failed to serialize document: {}", e),
                },
            },
            Err(e) => to_error_response(e),
        },
        OperationRequest::Find {
            db,
            collection,
            filter,
            options,
            ..
        } => match operations.find_raw(&db, &collection, filter, options).await {
            Ok((count, doc_bytes)) => OperationResponse::Batch { count, doc_bytes },
            Err(e) => to_error_response(e),
        },
        OperationRequest::InsertOne {
            db, collection, ..
        } => {
            let doc = match parse_single_bson_doc(raw_docs) {
                Ok(d) => d,
                Err(msg) => {
                    return OperationResponse::Error {
                        code: 1,
                        message: msg,
                    }
                }
            };
            match operations.insert(&db, &collection, doc).await {
                Ok(result) => OperationResponse::InsertOneResult {
                    inserted_id: result.inserted_id.to_string(),
                },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::InsertMany {
            db, collection, ..
        } => {
            let docs = match parse_bson_docs(raw_docs) {
                Ok(d) => d,
                Err(msg) => {
                    return OperationResponse::Error {
                        code: 1,
                        message: msg,
                    }
                }
            };
            match operations.insert_many(&db, &collection, docs).await {
                Ok(result) => OperationResponse::InsertManyResult {
                    inserted_count: result.inserted_ids.len() as u32,
                },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::UpdateOne {
            db,
            collection,
            filter,
            update,
        } => match operations.update(&db, &collection, filter, update).await {
            Ok(result) => OperationResponse::UpdateResult {
                matched_count: result.matched_count,
                modified_count: result.modified_count,
            },
            Err(e) => to_error_response(e),
        },
        OperationRequest::UpdateMany {
            db,
            collection,
            filter,
            update,
        } => match operations.update_many(&db, &collection, filter, update).await {
            Ok(result) => OperationResponse::UpdateResult {
                matched_count: result.matched_count,
                modified_count: result.modified_count,
            },
            Err(e) => to_error_response(e),
        },
        OperationRequest::DeleteOne {
            db,
            collection,
            filter,
        } => match operations.delete(&db, &collection, filter).await {
            Ok(result) => OperationResponse::DeleteResult {
                deleted_count: result.deleted_count,
            },
            Err(e) => to_error_response(e),
        },
        OperationRequest::DeleteMany {
            db,
            collection,
            filter,
        } => match operations.delete_many(&db, &collection, filter).await {
            Ok(result) => OperationResponse::DeleteResult {
                deleted_count: result.deleted_count,
            },
            Err(e) => to_error_response(e),
        },
        OperationRequest::Aggregate {
            db,
            collection,
            pipeline,
            ..
        } => match operations.aggregate_raw(&db, &collection, pipeline).await {
            Ok((count, doc_bytes)) => OperationResponse::Batch { count, doc_bytes },
            Err(e) => to_error_response(e),
        },
        OperationRequest::CountDocuments {
            db,
            collection,
            filter,
        } => match operations.count_documents(&db, &collection, filter).await {
            Ok(count) => OperationResponse::CountResult { count },
            Err(e) => to_error_response(e),
        },
        OperationRequest::RunCommand { db, command } => {
            let opts = RawCommandOptions::default();
            match run_command(pool, &db, command, &opts).await {
                Ok(doc) => match bson::to_vec(&doc) {
                    Ok(raw) => OperationResponse::CommandResult { doc: raw },
                    Err(e) => OperationResponse::Error {
                        code: 1,
                        message: format!("failed to serialize command result: {}", e),
                    },
                },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::CreateIndex {
            db,
            collection,
            keys,
            name,
            unique,
        } => {
            let opts = if name.is_some() || unique.is_some() {
                Some(IndexOptions {
                    name,
                    unique,
                    sparse: None,
                })
            } else {
                None
            };
            match operations.create_index(&db, &collection, keys, opts).await {
                Ok(index_name) => OperationResponse::IndexResult { name: index_name },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::ListCollections { db } => {
            let database = pool.database(&db);
            match database.list_collection_names().await {
                Ok(names) => OperationResponse::Collections { names },
                Err(e) => to_error_response(MongoCoreError::OperationError(e.to_string())),
            }
        }
        OperationRequest::CreateCollection { db, name } => {
            match operations.create_collection(&db, &name).await {
                Ok(()) => OperationResponse::SingleDoc {
                    ok: true,
                    doc: None,
                },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::DropCollection { db, name } => {
            match operations.drop_collection(&db, &name).await {
                Ok(()) => OperationResponse::SingleDoc {
                    ok: true,
                    doc: None,
                },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::Distinct {
            db,
            collection,
            field,
            filter,
        } => {
            let coll = pool.collection(&db, &collection);
            let result = coll.distinct(&field, filter.unwrap_or_default()).await;
            match result {
                Ok(values) => {
                    let doc = bson::doc! { "values": values };
                    match bson::to_vec(&doc) {
                        Ok(raw) => OperationResponse::CommandResult { doc: raw },
                        Err(e) => OperationResponse::Error {
                            code: 1,
                            message: format!("failed to serialize distinct result: {}", e),
                        },
                    }
                }
                Err(e) => to_error_response(MongoCoreError::OperationError(e.to_string())),
            }
        }
        OperationRequest::FindOneAndUpdate {
            db,
            collection,
            filter,
            update,
        } => match operations
            .find_and_modify(&db, &collection, filter, update, None)
            .await
        {
            Ok(doc) => match doc.map(|d| bson::to_vec(&d)).transpose() {
                Ok(raw) => OperationResponse::SingleDoc {
                    ok: true,
                    doc: raw,
                },
                Err(e) => OperationResponse::Error {
                    code: 1,
                    message: format!("failed to serialize document: {}", e),
                },
            },
            Err(e) => to_error_response(e),
        },
        OperationRequest::FindOneAndDelete {
            db,
            collection,
            filter,
        } => {
            // Use run_command with findAndModify + remove:true
            let cmd = bson::doc! {
                "findAndModify": &collection,
                "query": filter,
                "remove": true,
            };
            let opts = RawCommandOptions::default();
            match run_command(pool, &db, cmd, &opts).await {
                Ok(result) => {
                    let doc = result.get_document("value").ok().cloned();
                    match doc.map(|d| bson::to_vec(&d)).transpose() {
                        Ok(raw) => OperationResponse::SingleDoc {
                            ok: true,
                            doc: raw,
                        },
                        Err(e) => OperationResponse::Error {
                            code: 1,
                            message: format!("failed to serialize document: {}", e),
                        },
                    }
                }
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::BulkWrite {
            db, collection, ..
        } => {
            // BulkWrite: parse raw_docs as a sequence of BSON documents to insert
            let docs = match parse_bson_docs(raw_docs) {
                Ok(d) => d,
                Err(msg) => {
                    return OperationResponse::Error {
                        code: 1,
                        message: msg,
                    }
                }
            };
            match operations.insert_many(&db, &collection, docs).await {
                Ok(result) => OperationResponse::InsertManyResult {
                    inserted_count: result.inserted_ids.len() as u32,
                },
                Err(e) => to_error_response(e),
            }
        }
    }
}

/// Parse a single BSON document from a byte slice.
fn parse_single_bson_doc(raw: &[u8]) -> Result<Document, String> {
    if raw.len() < 5 {
        return Err("raw document bytes too short".to_string());
    }
    bson::from_slice::<Document>(raw).map_err(|e| format!("failed to parse BSON document: {}", e))
}

/// Parse concatenated BSON documents from a byte slice.
///
/// Each BSON document starts with a 4-byte little-endian length prefix.
fn parse_bson_docs(raw: &[u8]) -> Result<Vec<Document>, String> {
    let mut docs = Vec::new();
    let mut offset = 0;

    while offset < raw.len() {
        if raw.len() - offset < 4 {
            return Err("incomplete BSON length prefix".to_string());
        }
        let len =
            u32::from_le_bytes([raw[offset], raw[offset + 1], raw[offset + 2], raw[offset + 3]])
                as usize;
        if len < 5 {
            return Err("invalid BSON document length".to_string());
        }
        if offset + len > raw.len() {
            return Err("BSON document extends beyond buffer".to_string());
        }
        let doc = bson::from_slice::<Document>(&raw[offset..offset + len])
            .map_err(|e| format!("failed to parse BSON document at offset {}: {}", offset, e))?;
        docs.push(doc);
        offset += len;
    }

    Ok(docs)
}

/// Convert a MongoCoreError into an error OperationResponse.
fn to_error_response(err: MongoCoreError) -> OperationResponse {
    OperationResponse::Error {
        code: 1,
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_dispatch_handshake() {
        let req = OperationRequest::Handshake {
            client_language: "python".to_string(),
        };
        let response = handle_handshake(&req, 16 * 1024 * 1024, 128);
        match response {
            OperationResponse::Handshake {
                max_frame_size,
                max_concurrent,
            } => {
                assert_eq!(max_frame_size, 16 * 1024 * 1024);
                assert_eq!(max_concurrent, 128);
            }
            _ => panic!("expected Handshake response"),
        }
    }

    #[test]
    fn test_dispatch_handshake_wrong_request() {
        let req = OperationRequest::FindOne {
            db: "test".to_string(),
            collection: "coll".to_string(),
            filter: doc! {},
            projection: None,
        };
        let response = handle_handshake(&req, 16 * 1024 * 1024, 128);
        match response {
            OperationResponse::Error { code, message } => {
                assert_eq!(code, 1);
                assert!(message.contains("expected Handshake"));
            }
            _ => panic!("expected Error response"),
        }
    }

    #[test]
    fn test_parse_single_bson_doc() {
        let doc = doc! { "name": "Alice", "age": 30 };
        let bytes = bson::to_vec(&doc).unwrap();
        let parsed = parse_single_bson_doc(&bytes).unwrap();
        assert_eq!(parsed, doc);
    }

    #[test]
    fn test_parse_single_bson_doc_too_short() {
        let result = parse_single_bson_doc(&[0, 1, 2]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too short"));
    }

    #[test]
    fn test_parse_bson_docs_multiple() {
        let doc1 = doc! { "x": 1 };
        let doc2 = doc! { "y": 2 };
        let doc3 = doc! { "z": 3 };

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&bson::to_vec(&doc1).unwrap());
        bytes.extend_from_slice(&bson::to_vec(&doc2).unwrap());
        bytes.extend_from_slice(&bson::to_vec(&doc3).unwrap());

        let docs = parse_bson_docs(&bytes).unwrap();
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0], doc1);
        assert_eq!(docs[1], doc2);
        assert_eq!(docs[2], doc3);
    }

    #[test]
    fn test_parse_bson_docs_empty() {
        let docs = parse_bson_docs(&[]).unwrap();
        assert!(docs.is_empty());
    }

    #[test]
    fn test_parse_bson_docs_truncated() {
        let doc = doc! { "x": 1 };
        let bytes = bson::to_vec(&doc).unwrap();
        // Truncate the bytes
        let truncated = &bytes[..bytes.len() - 2];
        let result = parse_bson_docs(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_error_response() {
        let err = MongoCoreError::OperationError("something failed".to_string());
        let response = to_error_response(err);
        match response {
            OperationResponse::Error { code, message } => {
                assert_eq!(code, 1);
                assert!(message.contains("something failed"));
            }
            _ => panic!("expected Error response"),
        }
    }
}
