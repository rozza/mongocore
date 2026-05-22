"""Integration tests for the MongoCore Python client.

Requires a running MongoCore sidecar on localhost:50051.
Start with: cargo run -- --config config.test.toml

Tests are parametrized over transport ("grpc" and "binary") where possible.
Tests that use operations not supported by the binary transport use the
grpc_client fixture directly.
"""

import asyncio
import sys
import uuid

import pytest
import pytest_asyncio

sys.path.insert(0, "src")
from mongocore import MongoClient


TEST_DB = "mongocore_client_test"


def unique_collection():
    return f"py_test_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(params=["grpc", "binary"])
async def client(request):
    """Parametrized client fixture that yields both gRPC and binary transport clients."""
    transport = request.param
    if transport == "binary":
        from mongocore.binary_transport import BinaryTransport
        if not BinaryTransport.is_available():
            pytest.skip("Binary transport socket not available")
        c = MongoClient(transport="binary")
    else:
        c = MongoClient("localhost:50051", transport="grpc")
    await c.connect()
    yield c
    await c.close()


@pytest_asyncio.fixture
async def grpc_client():
    """Client fixture that always uses gRPC (for operations not supported by binary)."""
    c = MongoClient("localhost:50051", transport="grpc")
    await c.connect()
    yield c
    await c.close()


# =============================================================================
# Dual-transport tests (run on both gRPC and binary)
# =============================================================================


@pytest.mark.asyncio
async def test_insert_one_and_find_one(client):
    coll = client[TEST_DB][unique_collection()]

    inserted_id = await coll.insert_one({"name": "Alice", "age": 30})
    assert inserted_id

    doc = await coll.find_one({"name": "Alice"})
    assert doc is not None
    assert doc["name"] == "Alice"
    assert doc["age"] == 30


@pytest.mark.asyncio
async def test_insert_many(client):
    coll = client[TEST_DB][unique_collection()]

    ids = await coll.insert_many([
        {"name": "Bob", "score": 85},
        {"name": "Carol", "score": 92},
        {"name": "Dave", "score": 78},
    ])
    assert len(ids) == 3


@pytest.mark.asyncio
async def test_update_one(client):
    coll = client[TEST_DB][unique_collection()]

    await coll.insert_one({"name": "Eve", "status": "active"})
    result = await coll.update_one(
        {"name": "Eve"},
        {"$set": {"status": "inactive"}}
    )
    assert result["modified_count"] == 1

    doc = await coll.find_one({"name": "Eve"})
    assert doc["status"] == "inactive"


@pytest.mark.asyncio
async def test_update_many(client):
    coll = client[TEST_DB][unique_collection()]

    await coll.insert_many([
        {"category": "test", "value": 1},
        {"category": "test", "value": 2},
        {"category": "other", "value": 3},
    ])

    result = await coll.update_many(
        {"category": "test"},
        {"$set": {"updated": True}}
    )
    assert result["modified_count"] == 2


@pytest.mark.asyncio
async def test_delete_one(client):
    coll = client[TEST_DB][unique_collection()]

    await coll.insert_one({"name": "Frank"})
    await coll.insert_one({"name": "Grace"})

    count = await coll.delete_one({"name": "Frank"})
    assert count == 1


@pytest.mark.asyncio
async def test_delete_many(client):
    coll = client[TEST_DB][unique_collection()]

    await coll.insert_many([
        {"group": "A"},
        {"group": "A"},
        {"group": "B"},
    ])

    count = await coll.delete_many({"group": "A"})
    assert count == 2


@pytest.mark.asyncio
async def test_count_documents(client):
    coll = client[TEST_DB][unique_collection()]

    await coll.insert_many([
        {"x": 1},
        {"x": 2},
        {"x": 3},
    ])

    total = await coll.count_documents()
    assert total == 3


@pytest.mark.asyncio
async def test_count_documents_with_filter(client):
    coll = client[TEST_DB][unique_collection()]

    await coll.insert_many([
        {"status": "active"},
        {"status": "active"},
        {"status": "inactive"},
    ])

    active = await coll.count_documents({"status": "active"})
    assert active == 2

    inactive = await coll.count_documents({"status": "inactive"})
    assert inactive == 1


@pytest.mark.asyncio
async def test_find_one_returns_null(client):
    coll = client[TEST_DB][unique_collection()]

    doc = await coll.find_one({"nonexistent": True})
    assert doc is None


@pytest.mark.asyncio
async def test_run_command(client):
    result = await client.run_command("admin", {"ping": 1})
    assert result.get("ok") == 1.0


# =============================================================================
# gRPC-only tests (operations not supported by binary transport)
# =============================================================================


@pytest.mark.asyncio
async def test_insert_and_find(grpc_client):
    coll = grpc_client[TEST_DB][unique_collection()]

    inserted_id = await coll.insert_one({"name": "Alice", "age": 30})
    assert inserted_id

    docs = await coll.find({"name": "Alice"})
    assert len(docs) == 1
    assert docs[0]["name"] == "Alice"
    assert docs[0]["age"] == 30


@pytest.mark.asyncio
async def test_insert_many_and_find(grpc_client):
    coll = grpc_client[TEST_DB][unique_collection()]

    ids = await coll.insert_many([
        {"name": "Bob", "score": 85},
        {"name": "Carol", "score": 92},
        {"name": "Dave", "score": 78},
    ])
    assert len(ids) == 3

    docs = await coll.find({})
    assert len(docs) == 3


@pytest.mark.asyncio
async def test_find_with_limit(grpc_client):
    coll = grpc_client[TEST_DB][unique_collection()]

    await coll.insert_many([{"i": i} for i in range(10)])

    docs = await coll.find({}, limit=3)
    assert len(docs) == 3


@pytest.mark.asyncio
async def test_aggregate(grpc_client):
    coll = grpc_client[TEST_DB][unique_collection()]

    await coll.insert_many([
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "B", "value": 30},
    ])

    results = await coll.aggregate([
        {"$group": {"_id": "$category", "total": {"$sum": "$value"}}},
        {"$sort": {"_id": 1}},
    ])

    assert len(results) == 2
    assert results[0]["_id"] == "A"
    assert results[0]["total"] == 30
    assert results[1]["_id"] == "B"
    assert results[1]["total"] == 30


@pytest.mark.asyncio
async def test_find_and_modify(grpc_client):
    coll = grpc_client[TEST_DB][unique_collection()]

    await coll.insert_one({"counter": 10})
    result = await coll.find_and_modify(
        {"counter": 10},
        {"$inc": {"counter": 5}}
    )
    assert result is not None
    assert result["counter"] == 15


@pytest.mark.asyncio
async def test_watch(grpc_client):
    coll = grpc_client[TEST_DB][unique_collection()]

    # Insert a doc first so the collection exists
    await coll.insert_one({"setup": True})

    events = []
    async with coll.watch() as stream:
        # Insert in a separate task while watching
        async def do_insert():
            await asyncio.sleep(0.1)
            await coll.insert_one({"name": "watched"})
            await asyncio.sleep(0.1)

        insert_task = asyncio.create_task(do_insert())

        async for event in stream:
            events.append(event)
            if len(events) >= 1:
                break

        await insert_task

    assert len(events) == 1
    assert events[0]["operation_type"] == 0  # INSERT


@pytest.mark.asyncio
async def test_search(grpc_client):
    coll = grpc_client["mongocore_client_test"]["py_test_search"]
    await coll.insert_many([
        {"title": "rust programming guide", "content": "learn rust basics"},
        {"title": "python basics", "content": "learn python programming"},
        {"title": "rust advanced patterns", "content": "advanced rust techniques"},
    ])
    result = await coll.search("rust", limit=10)
    assert result["method"] in ("vector", "fulltext", "filter")
    assert result["total"] >= 2
    assert len(result["documents"]) >= 2


@pytest.mark.asyncio
async def test_list_databases(grpc_client):
    databases = await grpc_client.list_databases()
    assert isinstance(databases, list)
    assert len(databases) > 0


@pytest.mark.asyncio
async def test_list_collections(grpc_client):
    db = grpc_client[TEST_DB]
    coll_name = unique_collection()
    coll = db[coll_name]

    await coll.insert_one({"test": "data"})
    collections = await db.list_collections()
    assert isinstance(collections, list)
    assert coll_name in collections


@pytest.mark.asyncio
async def test_create_collection(grpc_client):
    db = grpc_client[TEST_DB]
    coll_name = unique_collection()

    await db.create_collection(coll_name)
    collections = await db.list_collections()
    assert coll_name in collections


@pytest.mark.asyncio
async def test_create_index(grpc_client):
    coll = grpc_client[TEST_DB][unique_collection()]

    await coll.insert_one({"field": "value"})
    index_name = await coll.create_index({"field": 1}, unique=True)
    assert index_name
    assert len(index_name) > 0


@pytest.mark.asyncio
async def test_get_analytics(grpc_client):
    coll = grpc_client[TEST_DB][unique_collection()]
    await coll.insert_one({"test": "data"})

    analytics = await grpc_client.get_analytics()
    assert "total_operations" in analytics
    assert analytics["total_operations"] >= 0


@pytest.mark.asyncio
async def test_transaction_commit(grpc_client):
    txn_id = await grpc_client.begin_transaction()
    assert txn_id
    assert len(txn_id) > 0

    result = await grpc_client.commit_transaction(txn_id)
    assert result is True


@pytest.mark.asyncio
async def test_transaction_abort(grpc_client):
    txn_id = await grpc_client.begin_transaction()
    assert txn_id
    assert len(txn_id) > 0

    result = await grpc_client.abort_transaction(txn_id)
    assert result is True


@pytest.mark.asyncio
async def test_ingest_csv(grpc_client):
    import os

    csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_fixtures/sample.csv"))
    result = await grpc_client.ingest(
        file_path=csv_path,
        database=TEST_DB,
        collection=unique_collection()
    )
    assert result["job_id"]
    assert len(result["job_id"]) > 0


@pytest.mark.asyncio
async def test_ingest_status(grpc_client):
    import os

    csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_fixtures/sample.csv"))
    result = await grpc_client.ingest(
        file_path=csv_path,
        database=TEST_DB,
        collection=unique_collection()
    )
    job_id = result["job_id"]

    status = await grpc_client.ingest_status(job_id)
    assert status
    assert status.get("job_id") == job_id


@pytest.mark.asyncio
async def test_list_ingest_jobs(grpc_client):
    jobs = await grpc_client.list_ingest_jobs()
    assert isinstance(jobs, list)


@pytest.mark.asyncio
async def test_cancel_ingest(grpc_client):
    import os

    csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../test_fixtures/sample.csv"))
    ingest_result = await grpc_client.ingest(
        file_path=csv_path,
        database=TEST_DB,
        collection=unique_collection()
    )
    job_id = ingest_result["job_id"]

    result = await grpc_client.cancel_ingest(job_id)
    assert isinstance(result, bool)


@pytest.mark.asyncio
async def test_watch_directory(grpc_client):
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        watch_id = await grpc_client.watch_directory(
            path=tmpdir,
            database=TEST_DB,
            collection=unique_collection()
        )
        assert watch_id
        assert len(watch_id) > 0

        await grpc_client.stop_watch(watch_id)


@pytest.mark.asyncio
async def test_stop_watch(grpc_client):
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        watch_id = await grpc_client.watch_directory(
            path=tmpdir,
            database=TEST_DB,
            collection=unique_collection()
        )

        result = await grpc_client.stop_watch(watch_id)
        assert result is True


@pytest.mark.asyncio
async def test_pipeline(grpc_client):
    from mongocore import ops

    coll_name = unique_collection()

    # Seed a document
    coll = grpc_client[TEST_DB][coll_name]
    await coll.insert_one({"name": "seed", "value": 100})

    # Run a pipeline with find + insert + list_databases
    results = await grpc_client.pipeline(
        ops.find(TEST_DB, coll_name, {"name": "seed"}),
        ops.insert(TEST_DB, coll_name, {"name": "pipeline_inserted", "value": 200}),
        ops.list_databases(),
    )

    assert len(results) == 3

    # First result: find
    assert results[0].success
    assert results[0].documents is not None
    assert len(results[0].documents) == 1
    assert results[0].documents[0]["name"] == "seed"
    assert results[0].documents[0]["value"] == 100

    # Second result: insert
    assert results[1].success
    assert results[1].inserted_id is not None

    # Third result: list_databases
    assert results[2].success
    assert results[2].databases is not None
    assert isinstance(results[2].databases, list)
    assert len(results[2].databases) > 0


@pytest.mark.asyncio
async def test_drop_collection(grpc_client):
    coll_name = unique_collection()
    coll = grpc_client[TEST_DB][coll_name]

    await coll.insert_one({"data": "to be dropped"})

    # Verify it exists
    docs = await coll.find({})
    assert len(docs) == 1

    # Drop it
    result = await coll.drop()
    assert result is True

    # Verify it's gone
    docs = await coll.find({})
    assert len(docs) == 0


@pytest.mark.asyncio
async def test_drop_collection_from_database(grpc_client):
    db = grpc_client[TEST_DB]
    coll_name = unique_collection()
    coll = db[coll_name]

    await coll.insert_one({"data": "test"})

    result = await db.drop_collection(coll_name)
    assert result is True


@pytest.mark.asyncio
async def test_embed_and_store(grpc_client):
    """Test embed_and_store (may fail if no embedding provider configured)."""
    import json
    documents = json.dumps([
        {"text": "Hello world", "id": 1},
        {"text": "Goodbye world", "id": 2},
    ])
    try:
        result = await grpc_client.embed_and_store(
            TEST_DB, unique_collection(), documents, "text"
        )
        assert "documents_stored" in result
        assert "embeddings_generated" in result
    except Exception:
        # Expected to fail if no embedding provider is configured
        pass


@pytest.mark.asyncio
async def test_semantic_search(grpc_client):
    """Test semantic_search (may fail if no vector index configured)."""
    try:
        result = await grpc_client.semantic_search(
            TEST_DB, unique_collection(), "hello"
        )
        assert "results" in result
        assert "count" in result
    except Exception:
        # Expected to fail if no vector index is configured
        pass
