//! BSON envelope codec for the binary UDS transport protocol.
//!
//! Parses BSON envelopes into typed `OperationRequest` variants and encodes
//! `OperationResponse` variants back into BSON bytes.

use bson::{doc, Document};

use crate::operations::FindOptions;
use crate::transport::frame::Opcode;

/// Errors that can occur during envelope codec operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// A required field is missing from the BSON envelope.
    MissingField(&'static str),
    /// The BSON envelope is malformed or cannot be deserialized.
    InvalidBson(String),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::MissingField(field) => write!(f, "missing required field: {}", field),
            CodecError::InvalidBson(msg) => write!(f, "invalid BSON: {}", msg),
        }
    }
}

impl std::error::Error for CodecError {}

/// Parsed operation request from a BSON envelope.
#[derive(Debug, Clone, PartialEq)]
pub enum OperationRequest {
    Handshake {
        client_language: String,
    },
    FindOne {
        db: String,
        collection: String,
        filter: Document,
        projection: Option<Document>,
    },
    Find {
        db: String,
        collection: String,
        filter: Document,
        options: Option<FindOptions>,
        batch_size: Option<u32>,
        doc_bytes_len: u32,
    },
    InsertOne {
        db: String,
        collection: String,
        doc_bytes_len: u32,
    },
    InsertMany {
        db: String,
        collection: String,
        ordered: bool,
        doc_bytes_len: u32,
    },
    UpdateOne {
        db: String,
        collection: String,
        filter: Document,
        update: Document,
    },
    UpdateMany {
        db: String,
        collection: String,
        filter: Document,
        update: Document,
    },
    DeleteOne {
        db: String,
        collection: String,
        filter: Document,
    },
    DeleteMany {
        db: String,
        collection: String,
        filter: Document,
    },
    Aggregate {
        db: String,
        collection: String,
        pipeline: Vec<Document>,
        batch_size: Option<u32>,
    },
    CountDocuments {
        db: String,
        collection: String,
        filter: Document,
    },
    RunCommand {
        db: String,
        command: Document,
    },
    CreateIndex {
        db: String,
        collection: String,
        keys: Document,
        name: Option<String>,
        unique: Option<bool>,
    },
    ListCollections {
        db: String,
    },
    CreateCollection {
        db: String,
        name: String,
    },
    DropCollection {
        db: String,
        name: String,
    },
    Distinct {
        db: String,
        collection: String,
        field: String,
        filter: Option<Document>,
    },
    FindOneAndUpdate {
        db: String,
        collection: String,
        filter: Document,
        update: Document,
    },
    FindOneAndDelete {
        db: String,
        collection: String,
        filter: Document,
    },
    BulkWrite {
        db: String,
        collection: String,
        doc_bytes_len: u32,
        ordered: bool,
    },
}

/// Encoded operation response.
#[derive(Debug, Clone, PartialEq)]
pub enum OperationResponse {
    Handshake {
        max_frame_size: u32,
        max_concurrent: u32,
    },
    SingleDoc {
        ok: bool,
        doc: Option<Vec<u8>>,
    },
    Batch {
        count: u32,
        doc_bytes: Vec<u8>,
    },
    InsertOneResult {
        inserted_id: String,
    },
    InsertManyResult {
        inserted_count: u32,
    },
    UpdateResult {
        matched_count: u64,
        modified_count: u64,
    },
    DeleteResult {
        deleted_count: u64,
    },
    CountResult {
        count: u64,
    },
    IndexResult {
        name: String,
    },
    Collections {
        names: Vec<String>,
    },
    CommandResult {
        doc: Vec<u8>,
    },
    Error {
        code: i32,
        message: String,
    },
    Shutdown {
        drain_ms: u32,
    },
}

/// Extract a required string field from a BSON document.
fn get_required_str(doc: &Document, field: &'static str) -> Result<String, CodecError> {
    doc.get_str(field)
        .map(|s| s.to_string())
        .map_err(|_| CodecError::MissingField(field))
}

/// Extract a required document field from a BSON document.
fn get_required_doc(doc: &Document, field: &'static str) -> Result<Document, CodecError> {
    doc.get_document(field)
        .cloned()
        .map_err(|_| CodecError::MissingField(field))
}

/// Parse a BSON envelope into an `OperationRequest` based on the opcode.
pub fn parse_envelope(bytes: &[u8], opcode: Opcode) -> Result<OperationRequest, CodecError> {
    let doc = bson::from_slice::<Document>(bytes)
        .map_err(|e| CodecError::InvalidBson(e.to_string()))?;

    match opcode {
        Opcode::Handshake => {
            let client_language = get_required_str(&doc, "client_language")?;
            Ok(OperationRequest::Handshake { client_language })
        }
        Opcode::FindOne => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = get_required_doc(&doc, "filter")?;
            let projection = doc.get_document("projection").ok().cloned();
            Ok(OperationRequest::FindOne {
                db,
                collection,
                filter,
                projection,
            })
        }
        Opcode::Find => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = get_required_doc(&doc, "filter")?;
            let doc_bytes_len = doc.get_i32("doc_bytes_len").unwrap_or(0) as u32;
            let batch_size = doc.get_i32("batch_size").ok().map(|v| v as u32);

            let options = {
                let limit = doc.get_i64("limit").ok();
                let skip = doc.get_i64("skip").ok().map(|v| v as u64);
                let sort = doc.get_document("sort").ok().cloned();
                let projection = doc.get_document("projection").ok().cloned();
                if limit.is_some() || skip.is_some() || sort.is_some() || projection.is_some() {
                    Some(FindOptions {
                        limit,
                        skip,
                        sort,
                        projection,
                    })
                } else {
                    None
                }
            };

            Ok(OperationRequest::Find {
                db,
                collection,
                filter,
                options,
                batch_size,
                doc_bytes_len,
            })
        }
        Opcode::InsertOne => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let doc_bytes_len = doc.get_i32("doc_bytes_len").unwrap_or(0) as u32;
            Ok(OperationRequest::InsertOne {
                db,
                collection,
                doc_bytes_len,
            })
        }
        Opcode::InsertMany => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let ordered = doc.get_bool("ordered").unwrap_or(true);
            let doc_bytes_len = doc.get_i32("doc_bytes_len").unwrap_or(0) as u32;
            Ok(OperationRequest::InsertMany {
                db,
                collection,
                ordered,
                doc_bytes_len,
            })
        }
        Opcode::UpdateOne => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = get_required_doc(&doc, "filter")?;
            let update = get_required_doc(&doc, "update")?;
            Ok(OperationRequest::UpdateOne {
                db,
                collection,
                filter,
                update,
            })
        }
        Opcode::UpdateMany => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = get_required_doc(&doc, "filter")?;
            let update = get_required_doc(&doc, "update")?;
            Ok(OperationRequest::UpdateMany {
                db,
                collection,
                filter,
                update,
            })
        }
        Opcode::DeleteOne => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = get_required_doc(&doc, "filter")?;
            Ok(OperationRequest::DeleteOne {
                db,
                collection,
                filter,
            })
        }
        Opcode::DeleteMany => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = get_required_doc(&doc, "filter")?;
            Ok(OperationRequest::DeleteMany {
                db,
                collection,
                filter,
            })
        }
        Opcode::Aggregate => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let pipeline_arr = doc
                .get_array("pipeline")
                .map_err(|_| CodecError::MissingField("pipeline"))?;
            let pipeline: Vec<Document> = pipeline_arr
                .iter()
                .filter_map(|v| v.as_document().cloned())
                .collect();
            let batch_size = doc.get_i32("batch_size").ok().map(|v| v as u32);
            Ok(OperationRequest::Aggregate {
                db,
                collection,
                pipeline,
                batch_size,
            })
        }
        Opcode::CountDocuments => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = doc.get_document("filter").cloned().unwrap_or_default();
            Ok(OperationRequest::CountDocuments {
                db,
                collection,
                filter,
            })
        }
        Opcode::Distinct => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let field = get_required_str(&doc, "field")?;
            let filter = doc.get_document("filter").ok().cloned();
            Ok(OperationRequest::Distinct {
                db,
                collection,
                field,
                filter,
            })
        }
        Opcode::CreateIndex => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let keys = get_required_doc(&doc, "keys")?;
            let name = doc.get_str("name").ok().map(|s| s.to_string());
            let unique = doc.get_bool("unique").ok();
            Ok(OperationRequest::CreateIndex {
                db,
                collection,
                keys,
                name,
                unique,
            })
        }
        Opcode::ListCollections => {
            let db = get_required_str(&doc, "db")?;
            Ok(OperationRequest::ListCollections { db })
        }
        Opcode::RunCommand => {
            let db = get_required_str(&doc, "db")?;
            let command = get_required_doc(&doc, "command")?;
            Ok(OperationRequest::RunCommand { db, command })
        }
        Opcode::BulkWrite => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let doc_bytes_len = doc.get_i32("doc_bytes_len").unwrap_or(0) as u32;
            let ordered = doc.get_bool("ordered").unwrap_or(true);
            Ok(OperationRequest::BulkWrite {
                db,
                collection,
                doc_bytes_len,
                ordered,
            })
        }
        Opcode::FindOneAndUpdate => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = get_required_doc(&doc, "filter")?;
            let update = get_required_doc(&doc, "update")?;
            Ok(OperationRequest::FindOneAndUpdate {
                db,
                collection,
                filter,
                update,
            })
        }
        Opcode::FindOneAndDelete => {
            let db = get_required_str(&doc, "db")?;
            let collection = get_required_str(&doc, "coll")?;
            let filter = get_required_doc(&doc, "filter")?;
            Ok(OperationRequest::FindOneAndDelete {
                db,
                collection,
                filter,
            })
        }
        Opcode::CreateCollection => {
            let db = get_required_str(&doc, "db")?;
            let name = get_required_str(&doc, "name")?;
            Ok(OperationRequest::CreateCollection { db, name })
        }
        Opcode::DropCollection => {
            let db = get_required_str(&doc, "db")?;
            let name = get_required_str(&doc, "name")?;
            Ok(OperationRequest::DropCollection { db, name })
        }
        Opcode::Error => {
            Err(CodecError::InvalidBson(
                "Error opcode is not a valid request".to_string(),
            ))
        }
    }
}

/// Encode an `OperationResponse` into (envelope_bytes, raw_doc_bytes).
///
/// The envelope always includes a `doc_bytes_len` field indicating the length
/// of the accompanying raw document bytes.
pub fn encode_response(response: &OperationResponse) -> Result<(Vec<u8>, Vec<u8>), CodecError> {
    let (envelope_doc, raw_bytes) = match response {
        OperationResponse::Handshake {
            max_frame_size,
            max_concurrent,
        } => {
            let d = doc! {
                "max_frame_size": *max_frame_size as i32,
                "max_concurrent": *max_concurrent as i32,
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::SingleDoc { ok, doc: raw_doc } => {
            let raw = raw_doc.clone().unwrap_or_default();
            let d = doc! {
                "ok": *ok,
                "doc_bytes_len": raw.len() as i32,
            };
            (d, raw)
        }
        OperationResponse::Batch { count, doc_bytes } => {
            let d = doc! {
                "count": *count as i32,
                "doc_bytes_len": doc_bytes.len() as i32,
            };
            (d, doc_bytes.clone())
        }
        OperationResponse::InsertOneResult { inserted_id } => {
            let d = doc! {
                "inserted_id": inserted_id.as_str(),
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::InsertManyResult { inserted_count } => {
            let d = doc! {
                "inserted_count": *inserted_count as i32,
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::UpdateResult {
            matched_count,
            modified_count,
        } => {
            let d = doc! {
                "matched_count": *matched_count as i64,
                "modified_count": *modified_count as i64,
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::DeleteResult { deleted_count } => {
            let d = doc! {
                "deleted_count": *deleted_count as i64,
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::CountResult { count } => {
            let d = doc! {
                "count": *count as i64,
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::IndexResult { name } => {
            let d = doc! {
                "name": name.as_str(),
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::Collections { names } => {
            let names_bson: Vec<bson::Bson> =
                names.iter().map(|n| bson::Bson::String(n.clone())).collect();
            let d = doc! {
                "names": names_bson,
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::CommandResult { doc: raw_doc } => {
            let d = doc! {
                "doc_bytes_len": raw_doc.len() as i32,
            };
            (d, raw_doc.clone())
        }
        OperationResponse::Error { code, message } => {
            let d = doc! {
                "code": *code,
                "message": message.as_str(),
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
        OperationResponse::Shutdown { drain_ms } => {
            let d = doc! {
                "drain_ms": *drain_ms as i32,
                "doc_bytes_len": 0_i32,
            };
            (d, Vec::new())
        }
    };

    let envelope_bytes =
        bson::to_vec(&envelope_doc).map_err(|e| CodecError::InvalidBson(e.to_string()))?;

    Ok((envelope_bytes, raw_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_find_one_envelope() {
        let envelope = doc! {
            "db": "testdb",
            "coll": "users",
            "filter": { "name": "Alice" },
            "projection": { "name": 1, "_id": 0 },
        };
        let bytes = bson::to_vec(&envelope).unwrap();

        let result = parse_envelope(&bytes, Opcode::FindOne).unwrap();
        match result {
            OperationRequest::FindOne {
                db,
                collection,
                filter,
                projection,
            } => {
                assert_eq!(db, "testdb");
                assert_eq!(collection, "users");
                assert_eq!(filter, doc! { "name": "Alice" });
                assert_eq!(projection, Some(doc! { "name": 1, "_id": 0 }));
            }
            _ => panic!("expected FindOne variant"),
        }
    }

    #[test]
    fn test_parse_insert_one_envelope() {
        let envelope = doc! {
            "db": "testdb",
            "coll": "items",
            "doc_bytes_len": 128_i32,
        };
        let bytes = bson::to_vec(&envelope).unwrap();

        let result = parse_envelope(&bytes, Opcode::InsertOne).unwrap();
        match result {
            OperationRequest::InsertOne {
                db,
                collection,
                doc_bytes_len,
            } => {
                assert_eq!(db, "testdb");
                assert_eq!(collection, "items");
                assert_eq!(doc_bytes_len, 128);
            }
            _ => panic!("expected InsertOne variant"),
        }
    }

    #[test]
    fn test_encode_find_one_response() {
        let sample_doc = doc! { "name": "Alice", "age": 30 };
        let raw_bytes = bson::to_vec(&sample_doc).unwrap();

        let response = OperationResponse::SingleDoc {
            ok: true,
            doc: Some(raw_bytes.clone()),
        };

        let (envelope_bytes, doc_bytes) = encode_response(&response).unwrap();

        // Verify envelope has doc_bytes_len
        let envelope: Document = bson::from_slice(&envelope_bytes).unwrap();
        assert_eq!(envelope.get_bool("ok"), Ok(true));
        assert_eq!(
            envelope.get_i32("doc_bytes_len").unwrap() as usize,
            raw_bytes.len()
        );

        // Verify raw doc bytes match
        assert_eq!(doc_bytes, raw_bytes);
    }

    #[test]
    fn test_encode_error_response() {
        let response = OperationResponse::Error {
            code: 11000,
            message: "duplicate key error".to_string(),
        };

        let (envelope_bytes, doc_bytes) = encode_response(&response).unwrap();

        let envelope: Document = bson::from_slice(&envelope_bytes).unwrap();
        assert_eq!(envelope.get_i32("code").unwrap(), 11000);
        assert_eq!(
            envelope.get_str("message").unwrap(),
            "duplicate key error"
        );
        assert_eq!(envelope.get_i32("doc_bytes_len").unwrap(), 0);
        assert!(doc_bytes.is_empty());
    }

    #[test]
    fn test_parse_missing_required_field() {
        let envelope = doc! {
            "db": "testdb",
            // missing "coll" and "filter"
        };
        let bytes = bson::to_vec(&envelope).unwrap();

        let result = parse_envelope(&bytes, Opcode::FindOne);
        assert!(result.is_err());
        match result.unwrap_err() {
            CodecError::MissingField(field) => assert_eq!(field, "coll"),
            other => panic!("expected MissingField, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_find_with_options() {
        let envelope = doc! {
            "db": "mydb",
            "coll": "orders",
            "filter": { "status": "pending" },
            "limit": 10_i64,
            "skip": 5_i64,
            "sort": { "created_at": -1 },
        };
        let bytes = bson::to_vec(&envelope).unwrap();

        let result = parse_envelope(&bytes, Opcode::Find).unwrap();
        match result {
            OperationRequest::Find {
                db,
                collection,
                filter,
                options,
                ..
            } => {
                assert_eq!(db, "mydb");
                assert_eq!(collection, "orders");
                assert_eq!(filter, doc! { "status": "pending" });
                let opts = options.unwrap();
                assert_eq!(opts.limit, Some(10));
                assert_eq!(opts.skip, Some(5));
                assert_eq!(opts.sort, Some(doc! { "created_at": -1 }));
            }
            _ => panic!("expected Find variant"),
        }
    }
}
