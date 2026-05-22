package mongocore_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/rozza/mongocore/clients/go/mongocore"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func getBinarySocketPath() string {
	if path := os.Getenv("MONGOCORE_BINARY_SOCKET_PATH"); path != "" {
		return path
	}
	return "/tmp/mongocore.bin.sock"
}

func skipIfNoBinarySocket(t *testing.T) {
	t.Helper()
	path := getBinarySocketPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Skipf("Binary transport socket not available at %s", path)
	}
}

func setupBinaryTransport(t *testing.T) *mongocore.BinaryTransport {
	t.Helper()
	skipIfNoBinarySocket(t)
	bt, err := mongocore.NewBinaryTransport(getBinarySocketPath())
	if err != nil {
		t.Fatalf("Failed to connect binary transport: %v", err)
	}
	t.Cleanup(func() { bt.Close() })
	return bt
}

func binaryCollection(suffix string) string {
	return fmt.Sprintf("go_binary_test_%d_%s", os.Getpid(), suffix)
}

func TestBinaryInsertAndFindOne(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("insert_find")

	// Insert a document
	id, err := bt.InsertOne(testDB, coll, bson.M{
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("InsertOne failed: %v", err)
	}
	if id == "" {
		t.Fatal("Expected non-empty inserted ID")
	}

	// Find it back
	doc, err := bt.FindOne(testDB, coll, bson.M{"name": "Alice"})
	if err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}
	if doc == nil {
		t.Fatal("Expected a document, got nil")
	}
	if doc["name"] != "Alice" {
		t.Fatalf("Expected name=Alice, got %v", doc["name"])
	}
}

func TestBinaryDeleteOne(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("delete")

	// Insert a document to delete
	_, err := bt.InsertOne(testDB, coll, bson.M{"name": "ToDelete"})
	if err != nil {
		t.Fatalf("InsertOne failed: %v", err)
	}

	// Delete it
	count, err := bt.DeleteOne(testDB, coll, bson.M{"name": "ToDelete"})
	if err != nil {
		t.Fatalf("DeleteOne failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected deleted_count=1, got %d", count)
	}

	// Verify it's gone
	doc, err := bt.FindOne(testDB, coll, bson.M{"name": "ToDelete"})
	if err != nil {
		t.Fatalf("FindOne after delete failed: %v", err)
	}
	if doc != nil {
		t.Fatal("Expected nil after delete, got a document")
	}
}

func TestBinaryCountDocuments(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("count")

	// Insert several documents
	docs := []bson.M{
		{"x": 1},
		{"x": 2},
		{"x": 3},
	}
	_, err := bt.InsertMany(testDB, coll, docs, true)
	if err != nil {
		t.Fatalf("InsertMany failed: %v", err)
	}

	// Count all
	count, err := bt.CountDocuments(testDB, coll, bson.M{})
	if err != nil {
		t.Fatalf("CountDocuments failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("Expected count=3, got %d", count)
	}
}

func TestBinaryCountDocumentsWithFilter(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("count_filter")

	// Insert mixed documents
	docs := []bson.M{
		{"status": "active"},
		{"status": "active"},
		{"status": "inactive"},
	}
	_, err := bt.InsertMany(testDB, coll, docs, true)
	if err != nil {
		t.Fatalf("InsertMany failed: %v", err)
	}

	// Count with filter
	active, err := bt.CountDocuments(testDB, coll, bson.M{"status": "active"})
	if err != nil {
		t.Fatalf("CountDocuments with filter failed: %v", err)
	}
	if active != 2 {
		t.Fatalf("Expected count=2, got %d", active)
	}

	inactive, err := bt.CountDocuments(testDB, coll, bson.M{"status": "inactive"})
	if err != nil {
		t.Fatalf("CountDocuments with filter failed: %v", err)
	}
	if inactive != 1 {
		t.Fatalf("Expected count=1, got %d", inactive)
	}
}

func TestBinaryInsertMany(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("insert_many")

	docs := []bson.M{
		{"name": "doc1", "val": 1},
		{"name": "doc2", "val": 2},
		{"name": "doc3", "val": 3},
		{"name": "doc4", "val": 4},
		{"name": "doc5", "val": 5},
	}

	count, err := bt.InsertMany(testDB, coll, docs, true)
	if err != nil {
		t.Fatalf("InsertMany failed: %v", err)
	}
	if count != 5 {
		t.Fatalf("Expected inserted_count=5, got %d", count)
	}

	// Verify via CountDocuments
	total, err := bt.CountDocuments(testDB, coll, bson.M{})
	if err != nil {
		t.Fatalf("CountDocuments failed: %v", err)
	}
	if total != 5 {
		t.Fatalf("Expected 5 documents, got %d", total)
	}
}

func TestBinaryUpdateOne(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("update_one")

	// Insert a document
	_, err := bt.InsertOne(testDB, coll, bson.M{"name": "original", "status": "pending"})
	if err != nil {
		t.Fatalf("InsertOne failed: %v", err)
	}

	// Update it
	matched, modified, err := bt.UpdateOne(testDB, coll, bson.M{"name": "original"}, bson.M{"$set": bson.M{"status": "done"}})
	if err != nil {
		t.Fatalf("UpdateOne failed: %v", err)
	}
	if matched != 1 {
		t.Fatalf("Expected matched=1, got %d", matched)
	}
	if modified != 1 {
		t.Fatalf("Expected modified=1, got %d", modified)
	}

	// Verify via FindOne
	doc, err := bt.FindOne(testDB, coll, bson.M{"name": "original"})
	if err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}
	if doc["status"] != "done" {
		t.Fatalf("Expected status=done, got %v", doc["status"])
	}
}

func TestBinaryUpdateMany(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("update_many")

	// Insert 3 documents
	docs := []bson.M{
		{"group": "A", "val": 1},
		{"group": "A", "val": 2},
		{"group": "A", "val": 3},
	}
	insertCount, err := bt.InsertMany(testDB, coll, docs, true)
	if err != nil {
		t.Fatalf("InsertMany failed: %v", err)
	}
	if insertCount != 3 {
		t.Fatalf("Expected inserted_count=3, got %d", insertCount)
	}

	// Update all
	matched, modified, err := bt.UpdateMany(testDB, coll, bson.M{"group": "A"}, bson.M{"$set": bson.M{"group": "B"}})
	if err != nil {
		t.Fatalf("UpdateMany failed: %v", err)
	}
	if matched != 3 {
		t.Fatalf("Expected matched=3, got %d", matched)
	}
	if modified != 3 {
		t.Fatalf("Expected modified=3, got %d", modified)
	}
}

func TestBinaryDeleteMany(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("delete_many")

	// Insert 3 documents
	docs := []bson.M{
		{"item": "x"},
		{"item": "x"},
		{"item": "x"},
	}
	_, err := bt.InsertMany(testDB, coll, docs, true)
	if err != nil {
		t.Fatalf("InsertMany failed: %v", err)
	}

	// Delete all
	deleted, err := bt.DeleteMany(testDB, coll, bson.M{"item": "x"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("Expected deleted=3, got %d", deleted)
	}

	// Verify count is 0
	count, err := bt.CountDocuments(testDB, coll, bson.M{})
	if err != nil {
		t.Fatalf("CountDocuments failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("Expected count=0, got %d", count)
	}
}

func TestBinaryRunCommand(t *testing.T) {
	bt := setupBinaryTransport(t)

	result, err := bt.RunCommand(testDB, bson.M{"ping": 1})
	if err != nil {
		t.Fatalf("RunCommand failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// ping should return ok: 1
	ok, exists := result["ok"]
	if !exists {
		t.Fatal("Expected 'ok' field in ping response")
	}
	// ok can be float64(1) or int32(1)
	switch v := ok.(type) {
	case float64:
		if v != 1.0 {
			t.Fatalf("Expected ok=1, got %v", v)
		}
	case int32:
		if v != 1 {
			t.Fatalf("Expected ok=1, got %v", v)
		}
	default:
		t.Fatalf("Unexpected type for ok: %T = %v", ok, ok)
	}
}

func TestBinaryFindOneReturnsNull(t *testing.T) {
	bt := setupBinaryTransport(t)
	coll := binaryCollection("find_null")

	// Query a non-existent document
	doc, err := bt.FindOne(testDB, coll, bson.M{"nonexistent": true})
	if err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}
	if doc != nil {
		t.Fatal("Expected nil for non-existent document, got a document")
	}
}
