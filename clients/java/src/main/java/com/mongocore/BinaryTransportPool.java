package com.mongocore;

import org.bson.BsonDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection pool for BinaryTransport with round-robin dispatch.
 *
 * <p>Creates multiple connections to MongoCore's binary protocol and distributes
 * requests across them for improved throughput under concurrent workloads.</p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * try (BinaryTransportPool pool = new BinaryTransportPool()) {
 *     pool.connect();
 *     BsonDocument doc = pool.findOne("mydb", "mycoll", new BsonDocument());
 * }
 * }</pre>
 */
public class BinaryTransportPool implements AutoCloseable {

    private final List<BinaryTransport> connections = new ArrayList<>();
    private final AtomicInteger index = new AtomicInteger(0);
    private final String socketPath;
    private final int poolSize;

    /**
     * Creates a pool with the specified socket path and size.
     *
     * @param socketPath path to the Unix domain socket, or null for default
     * @param poolSize   number of connections in the pool
     */
    public BinaryTransportPool(String socketPath, int poolSize) {
        this.socketPath = socketPath != null ? socketPath : "/tmp/mongocore.bin.sock";
        this.poolSize = poolSize > 0 ? poolSize : 4;
    }

    /**
     * Creates a pool with default socket path and pool size of 4.
     */
    public BinaryTransportPool() {
        this(null, 4);
    }

    /**
     * Connects all pool members to the MongoCore binary socket.
     *
     * @throws IOException if any connection fails
     */
    public void connect() throws IOException {
        for (int i = 0; i < poolSize; i++) {
            BinaryTransport conn = new BinaryTransport(socketPath);
            conn.connect();
            connections.add(conn);
        }
    }

    /**
     * Closes all connections in the pool.
     *
     * @throws IOException if closing any connection fails
     */
    @Override
    public void close() throws IOException {
        IOException firstException = null;
        for (BinaryTransport conn : connections) {
            try {
                conn.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                }
            }
        }
        connections.clear();
        if (firstException != null) {
            throw firstException;
        }
    }

    private BinaryTransport next() {
        int idx = index.getAndIncrement();
        return connections.get(Math.floorMod(idx, connections.size()));
    }

    /**
     * Finds a single document matching the filter.
     *
     * @param db         database name
     * @param collection collection name
     * @param filter     BSON filter document
     * @return the matching document, or null if not found
     * @throws IOException if communication fails
     */
    public BsonDocument findOne(String db, String collection, BsonDocument filter) throws IOException {
        return next().findOne(db, collection, filter);
    }

    /**
     * Inserts a single document.
     *
     * @param db         database name
     * @param collection collection name
     * @param document   BSON document to insert
     * @return the inserted document's ID as a string
     * @throws IOException if communication fails
     */
    public String insertOne(String db, String collection, BsonDocument document) throws IOException {
        return next().insertOne(db, collection, document);
    }

    /**
     * Inserts multiple documents.
     *
     * @param db         database name
     * @param collection collection name
     * @param documents  list of BSON documents to insert
     * @param ordered    whether inserts should be ordered
     * @return the number of documents inserted
     * @throws IOException if communication fails
     */
    public int insertMany(String db, String collection, List<BsonDocument> documents, boolean ordered) throws IOException {
        return next().insertMany(db, collection, documents, ordered);
    }

    /**
     * Deletes a single document matching the filter.
     *
     * @param db         database name
     * @param collection collection name
     * @param filter     BSON filter document
     * @return the number of documents deleted (0 or 1)
     * @throws IOException if communication fails
     */
    public long deleteOne(String db, String collection, BsonDocument filter) throws IOException {
        return next().deleteOne(db, collection, filter);
    }

    /**
     * Counts documents matching the filter.
     *
     * @param db         database name
     * @param collection collection name
     * @param filter     BSON filter document
     * @return the count of matching documents
     * @throws IOException if communication fails
     */
    public long countDocuments(String db, String collection, BsonDocument filter) throws IOException {
        return next().countDocuments(db, collection, filter);
    }

    /**
     * Updates a single document matching the filter.
     *
     * @param db         database name
     * @param collection collection name
     * @param filter     BSON filter document
     * @param update     BSON update document
     * @return an UpdateResult with matchedCount and modifiedCount
     * @throws IOException if communication fails
     */
    public BinaryTransport.UpdateResult updateOne(String db, String collection, BsonDocument filter, BsonDocument update) throws IOException {
        return next().updateOne(db, collection, filter, update);
    }

    /**
     * Updates multiple documents matching the filter.
     *
     * @param db         database name
     * @param collection collection name
     * @param filter     BSON filter document
     * @param update     BSON update document
     * @return an UpdateResult with matchedCount and modifiedCount
     * @throws IOException if communication fails
     */
    public BinaryTransport.UpdateResult updateMany(String db, String collection, BsonDocument filter, BsonDocument update) throws IOException {
        return next().updateMany(db, collection, filter, update);
    }

    /**
     * Deletes multiple documents matching the filter.
     *
     * @param db         database name
     * @param collection collection name
     * @param filter     BSON filter document
     * @return the number of documents deleted
     * @throws IOException if communication fails
     */
    public long deleteMany(String db, String collection, BsonDocument filter) throws IOException {
        return next().deleteMany(db, collection, filter);
    }

    /**
     * Runs a database command.
     *
     * @param db      database name
     * @param command BSON command document
     * @return the command result as a BsonDocument
     * @throws IOException if communication fails
     */
    public BsonDocument runCommand(String db, BsonDocument command) throws IOException {
        return next().runCommand(db, command);
    }
}
