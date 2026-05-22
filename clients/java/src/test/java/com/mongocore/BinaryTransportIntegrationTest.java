package com.mongocore;

import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.BsonInt32;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class BinaryTransportIntegrationTest {

    private static final String BINARY_SOCKET = "/tmp/mongocore.bin.sock";
    private static final String TEST_DB = "mongocore_client_test";
    private static BinaryTransport transport;

    @BeforeClass
    public static void setUp() throws IOException {
        Assume.assumeTrue("Binary transport socket not available",
            Files.exists(Path.of(BINARY_SOCKET)));
        transport = new BinaryTransport(BINARY_SOCKET);
        transport.connect();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        if (transport != null) transport.close();
    }

    private static String uniqueCollection() {
        return "java_bin_" + UUID.randomUUID().toString().substring(0, 12);
    }

    @Test
    public void testInsertAndFindOne() throws IOException {
        String coll = uniqueCollection();

        BsonDocument doc = new BsonDocument()
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30));

        String insertedId = transport.insertOne(TEST_DB, coll, doc);
        assertNotNull("insertOne should return an inserted ID", insertedId);
        assertFalse("inserted ID should not be empty", insertedId.isEmpty());

        BsonDocument found = transport.findOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Alice")));
        assertNotNull("findOne should return the inserted document", found);
        assertEquals("Alice", found.getString("name").getValue());
        assertEquals(30, found.getInt32("age").getValue());
    }

    @Test
    public void testFindOneReturnsNullForNoMatch() throws IOException {
        String coll = uniqueCollection();

        BsonDocument missing = transport.findOne(TEST_DB, coll, new BsonDocument("nonexistent", new BsonString("value")));
        assertNull("findOne should return null when no document matches", missing);
    }

    @Test
    public void testInsertMany() throws IOException {
        String coll = uniqueCollection();

        List<BsonDocument> docs = List.of(
                new BsonDocument("name", new BsonString("Bob")).append("score", new BsonInt32(85)),
                new BsonDocument("name", new BsonString("Carol")).append("score", new BsonInt32(92)),
                new BsonDocument("name", new BsonString("Dave")).append("score", new BsonInt32(78))
        );

        int insertedCount = transport.insertMany(TEST_DB, coll, docs, true);
        assertEquals("insertMany should return count of 3", 3, insertedCount);

        // Verify documents exist
        BsonDocument bob = transport.findOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Bob")));
        assertNotNull("Should find Bob after insertMany", bob);
        assertEquals(85, bob.getInt32("score").getValue());
    }

    @Test
    public void testDeleteOne() throws IOException {
        String coll = uniqueCollection();

        transport.insertOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Frank")));
        transport.insertOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Grace")));

        long deleted = transport.deleteOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Frank")));
        assertEquals("deleteOne should delete exactly 1 document", 1, deleted);

        // Verify Frank is gone
        BsonDocument frank = transport.findOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Frank")));
        assertNull("Frank should no longer exist after deleteOne", frank);

        // Verify Grace still exists
        BsonDocument grace = transport.findOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Grace")));
        assertNotNull("Grace should still exist after deleting Frank", grace);
    }

    @Test
    public void testCountDocuments() throws IOException {
        String coll = uniqueCollection();

        List<BsonDocument> docs = List.of(
                new BsonDocument("status", new BsonString("active")),
                new BsonDocument("status", new BsonString("active")),
                new BsonDocument("status", new BsonString("inactive"))
        );
        transport.insertMany(TEST_DB, coll, docs, true);

        long total = transport.countDocuments(TEST_DB, coll, new BsonDocument());
        assertEquals("countDocuments with empty filter should return 3", 3, total);
    }

    @Test
    public void testCountWithFilter() throws IOException {
        String coll = uniqueCollection();

        List<BsonDocument> docs = List.of(
                new BsonDocument("status", new BsonString("active")),
                new BsonDocument("status", new BsonString("active")),
                new BsonDocument("status", new BsonString("inactive"))
        );
        transport.insertMany(TEST_DB, coll, docs, true);

        long active = transport.countDocuments(TEST_DB, coll, new BsonDocument("status", new BsonString("active")));
        assertEquals("countDocuments with filter should return 2 active documents", 2, active);

        long inactive = transport.countDocuments(TEST_DB, coll, new BsonDocument("status", new BsonString("inactive")));
        assertEquals("countDocuments with filter should return 1 inactive document", 1, inactive);
    }

    @Test
    public void testUpdateOne() throws IOException {
        String coll = uniqueCollection();

        transport.insertOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Helen")).append("score", new BsonInt32(50)));
        transport.insertOne(TEST_DB, coll, new BsonDocument("name", new BsonString("Helen")).append("score", new BsonInt32(60)));

        BsonDocument filter = new BsonDocument("name", new BsonString("Helen"));
        BsonDocument update = new BsonDocument("$set", new BsonDocument("score", new BsonInt32(99)));

        BinaryTransport.UpdateResult result = transport.updateOne(TEST_DB, coll, filter, update);
        assertEquals("updateOne should match 1 document", 1, result.matchedCount());
        assertEquals("updateOne should modify 1 document", 1, result.modifiedCount());

        // Verify only one was updated
        long count99 = transport.countDocuments(TEST_DB, coll, new BsonDocument("score", new BsonInt32(99)));
        assertEquals("Only one document should have score 99", 1, count99);
    }

    @Test
    public void testUpdateMany() throws IOException {
        String coll = uniqueCollection();

        List<BsonDocument> docs = List.of(
                new BsonDocument("team", new BsonString("red")).append("points", new BsonInt32(10)),
                new BsonDocument("team", new BsonString("red")).append("points", new BsonInt32(20)),
                new BsonDocument("team", new BsonString("blue")).append("points", new BsonInt32(30))
        );
        transport.insertMany(TEST_DB, coll, docs, true);

        BsonDocument filter = new BsonDocument("team", new BsonString("red"));
        BsonDocument update = new BsonDocument("$set", new BsonDocument("points", new BsonInt32(0)));

        BinaryTransport.UpdateResult result = transport.updateMany(TEST_DB, coll, filter, update);
        assertEquals("updateMany should match 2 documents", 2, result.matchedCount());
        assertEquals("updateMany should modify 2 documents", 2, result.modifiedCount());

        long zeroPoints = transport.countDocuments(TEST_DB, coll, new BsonDocument("points", new BsonInt32(0)));
        assertEquals("Two documents should have points=0 after updateMany", 2, zeroPoints);
    }

    @Test
    public void testDeleteMany() throws IOException {
        String coll = uniqueCollection();

        List<BsonDocument> docs = List.of(
                new BsonDocument("type", new BsonString("temp")),
                new BsonDocument("type", new BsonString("temp")),
                new BsonDocument("type", new BsonString("perm"))
        );
        transport.insertMany(TEST_DB, coll, docs, true);

        long deleted = transport.deleteMany(TEST_DB, coll, new BsonDocument("type", new BsonString("temp")));
        assertEquals("deleteMany should delete 2 documents", 2, deleted);

        long remaining = transport.countDocuments(TEST_DB, coll, new BsonDocument());
        assertEquals("Only 1 document should remain after deleteMany", 1, remaining);
    }

    @Test
    public void testRunCommand() throws IOException {
        BsonDocument command = new BsonDocument("ping", new BsonInt32(1));
        BsonDocument result = transport.runCommand(TEST_DB, command);
        assertNotNull("runCommand(ping) should return a result", result);
        // ping returns {"ok": 1.0}
        assertTrue("ping result should contain 'ok'", result.containsKey("ok"));
        assertEquals("ping should return ok=1.0", 1.0, result.getDouble("ok").getValue(), 0.001);
    }
}
