package com.mongocore;

import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.*;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

/**
 * Binary transport client for MongoCore over Unix Domain Sockets.
 *
 * <p>Provides a lightweight, high-performance binary protocol alternative to gRPC
 * for local communication with the MongoCore sidecar.</p>
 *
 * <p>Requires Java 16+ for {@link UnixDomainSocketAddress} support.</p>
 */
public class BinaryTransport implements AutoCloseable {

    private static final int HEADER_SIZE = 10;
    private static final String DEFAULT_SOCKET_PATH = "/tmp/mongocore.bin.sock";
    private static final int DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

    // Opcodes
    private static final int OP_FIND_ONE = 0x02;
    private static final int OP_INSERT_ONE = 0x03;
    private static final int OP_INSERT_MANY = 0x04;
    private static final int OP_UPDATE_ONE = 0x05;
    private static final int OP_UPDATE_MANY = 0x06;
    private static final int OP_DELETE_ONE = 0x07;
    private static final int OP_DELETE_MANY = 0x08;
    private static final int OP_COUNT = 0x0A;
    private static final int OP_RUN_COMMAND = 0x0D;
    private static final int OP_HANDSHAKE = 0x3F;

    // Flag bits
    private static final int FLAG_EOS = 1 << 6;
    private static final int FLAG_NO_REPLY = 1 << 7;

    private final String socketPath;
    private SocketChannel channel;
    private final AtomicInteger reqId = new AtomicInteger(0);
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    /**
     * Creates a new BinaryTransport with the specified socket path.
     *
     * @param socketPath path to the Unix domain socket
     */
    public BinaryTransport(String socketPath) {
        this.socketPath = socketPath != null ? socketPath : resolveSocketPath();
    }

    /**
     * Creates a new BinaryTransport using the default or environment-configured socket path.
     */
    public BinaryTransport() {
        this(null);
    }

    /**
     * Connects to the MongoCore binary transport and performs handshake.
     *
     * @throws IOException if connection or handshake fails
     */
    public void connect() throws IOException {
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
        channel = SocketChannel.open(address);
        channel.configureBlocking(true);
        handshake();
    }

    /**
     * Closes the connection.
     *
     * @throws IOException if closing fails
     */
    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
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
        BsonDocument envelope = new BsonDocument()
                .append("db", new BsonString(db))
                .append("coll", new BsonString(collection))
                .append("filter", filter);

        sendFrame(OP_FIND_ONE, envelope, null, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Server error: " + respEnvelope.getString("error").getValue());
        }

        if (response.rawDocs != null && response.rawDocs.length > 0) {
            return decodeBson(response.rawDocs);
        }
        return null;
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
        BsonDocument envelope = new BsonDocument()
                .append("db", new BsonString(db))
                .append("coll", new BsonString(collection));

        byte[] docBytes = encodeBson(document);
        sendFrame(OP_INSERT_ONE, envelope, docBytes, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Server error: " + respEnvelope.getString("error").getValue());
        }

        if (respEnvelope.containsKey("inserted_id")) {
            return respEnvelope.get("inserted_id").toString();
        }
        return null;
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
        BsonDocument envelope = new BsonDocument()
                .append("db", new BsonString(db))
                .append("coll", new BsonString(collection))
                .append("ordered", new BsonBoolean(ordered));

        // Concatenate all document bytes
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        for (BsonDocument doc : documents) {
            byte[] docBytes = encodeBson(doc);
            buffer.write(docBytes);
        }
        byte[] rawDocs = extractBytes(buffer);

        sendFrame(OP_INSERT_MANY, envelope, rawDocs, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Server error: " + respEnvelope.getString("error").getValue());
        }

        if (respEnvelope.containsKey("inserted_count")) {
            return respEnvelope.getInt32("inserted_count").getValue();
        }
        return 0;
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
        BsonDocument envelope = new BsonDocument()
                .append("db", new BsonString(db))
                .append("coll", new BsonString(collection))
                .append("filter", filter);

        sendFrame(OP_DELETE_ONE, envelope, null, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Server error: " + respEnvelope.getString("error").getValue());
        }

        if (respEnvelope.containsKey("deleted_count")) {
            return respEnvelope.getInt64("deleted_count").getValue();
        }
        return 0;
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
        BsonDocument envelope = new BsonDocument()
                .append("db", new BsonString(db))
                .append("coll", new BsonString(collection))
                .append("filter", filter);

        sendFrame(OP_COUNT, envelope, null, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Server error: " + respEnvelope.getString("error").getValue());
        }

        if (respEnvelope.containsKey("count")) {
            return respEnvelope.getInt64("count").getValue();
        }
        return 0;
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
    public UpdateResult updateOne(String db, String collection, BsonDocument filter, BsonDocument update) throws IOException {
        return doUpdate(OP_UPDATE_ONE, db, collection, filter, update);
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
    public UpdateResult updateMany(String db, String collection, BsonDocument filter, BsonDocument update) throws IOException {
        return doUpdate(OP_UPDATE_MANY, db, collection, filter, update);
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
        BsonDocument envelope = new BsonDocument()
                .append("db", new BsonString(db))
                .append("coll", new BsonString(collection))
                .append("filter", filter);

        sendFrame(OP_DELETE_MANY, envelope, null, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Server error: " + respEnvelope.getString("error").getValue());
        }

        if (respEnvelope.containsKey("deleted_count")) {
            return respEnvelope.getInt64("deleted_count").getValue();
        }
        return 0;
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
        BsonDocument envelope = new BsonDocument()
                .append("db", new BsonString(db))
                .append("command", command);

        sendFrame(OP_RUN_COMMAND, envelope, null, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Server error: " + respEnvelope.getString("error").getValue());
        }

        if (response.rawDocs != null && response.rawDocs.length > 0) {
            return decodeBson(response.rawDocs);
        }
        return respEnvelope;
    }

    /**
     * Result of an update operation.
     */
    public record UpdateResult(long matchedCount, long modifiedCount) {}

    /**
     * Checks if the binary transport socket is available.
     *
     * @param socketPath path to check, or null for default
     * @return true if the socket file exists
     */
    public static boolean isAvailable(String socketPath) {
        String path = socketPath != null ? socketPath : resolveSocketPath();
        return Files.exists(Path.of(path));
    }

    // --- Internal methods ---

    private UpdateResult doUpdate(int opcode, String db, String collection, BsonDocument filter, BsonDocument update) throws IOException {
        BsonDocument envelope = new BsonDocument()
                .append("db", new BsonString(db))
                .append("coll", new BsonString(collection))
                .append("filter", filter)
                .append("update", update);

        sendFrame(opcode, envelope, null, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Server error: " + respEnvelope.getString("error").getValue());
        }

        long matched = respEnvelope.containsKey("matched_count") ? respEnvelope.getInt64("matched_count").getValue() : 0;
        long modified = respEnvelope.containsKey("modified_count") ? respEnvelope.getInt64("modified_count").getValue() : 0;
        return new UpdateResult(matched, modified);
    }

    private void handshake() throws IOException {
        BsonDocument envelope = new BsonDocument()
                .append("client_language", new BsonString("java"));

        sendFrame(OP_HANDSHAKE, envelope, null, false);
        FrameResponse response = readFrame();

        BsonDocument respEnvelope = decodeBson(response.envelope);
        if (respEnvelope.containsKey("error")) {
            throw new IOException("Handshake failed: " + respEnvelope.getString("error").getValue());
        }

        if (respEnvelope.containsKey("max_frame_size")) {
            maxFrameSize = respEnvelope.getInt32("max_frame_size").getValue();
        }
    }

    private int nextReqId() {
        return reqId.incrementAndGet();
    }

    private void sendFrame(int opcode, BsonDocument envelope, byte[] rawDocs, boolean noReply) throws IOException {
        byte[] envelopeBytes = encodeBson(envelope);
        int rawDocsLen = rawDocs != null ? rawDocs.length : 0;

        // msg_len = flags(2) + req_id(4) + envelope + rawDocs
        int msgLen = 2 + 4 + envelopeBytes.length + rawDocsLen;

        if (msgLen + 4 > maxFrameSize) {
            throw new IOException("Frame exceeds max frame size: " + (msgLen + 4) + " > " + maxFrameSize);
        }

        // Build flags: bits[0:5] = opcode, bit6 = EOS (always set for request), bit7 = no-reply
        int flags = (opcode & 0x3F) | FLAG_EOS;
        if (noReply) {
            flags |= FLAG_NO_REPLY;
        }

        int id = nextReqId();

        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.order(ByteOrder.BIG_ENDIAN);
        header.putInt(msgLen);
        header.putShort((short) flags);
        header.putInt(id);
        header.flip();

        ByteBuffer payload = ByteBuffer.allocate(envelopeBytes.length + rawDocsLen);
        payload.put(envelopeBytes);
        if (rawDocs != null) {
            payload.put(rawDocs);
        }
        payload.flip();

        channel.write(header);
        channel.write(payload);
    }

    private FrameResponse readFrame() throws IOException {
        // Read header
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.order(ByteOrder.BIG_ENDIAN);
        readExact(header);
        header.flip();

        int msgLen = header.getInt();
        @SuppressWarnings("unused")
        short flags = header.getShort();
        @SuppressWarnings("unused")
        int respReqId = header.getInt();

        // Read payload: msgLen - 2 (flags) - 4 (req_id) = envelope + rawDocs
        int payloadLen = msgLen - 6;
        if (payloadLen < 4) {
            throw new IOException("Invalid frame: payload too small (" + payloadLen + " bytes)");
        }

        ByteBuffer payload = ByteBuffer.allocate(payloadLen);
        readExact(payload);
        payload.flip();

        // First 4 bytes (LE) = BSON envelope size
        payload.order(ByteOrder.LITTLE_ENDIAN);
        int envelopeSize = payload.getInt(0);
        payload.order(ByteOrder.BIG_ENDIAN);

        if (envelopeSize > payloadLen) {
            throw new IOException("Invalid frame: envelope size (" + envelopeSize + ") exceeds payload (" + payloadLen + ")");
        }

        byte[] envelopeBytes = new byte[envelopeSize];
        payload.get(envelopeBytes);

        byte[] rawDocs = null;
        int remaining = payloadLen - envelopeSize;
        if (remaining > 0) {
            rawDocs = new byte[remaining];
            payload.get(rawDocs);
        }

        return new FrameResponse(envelopeBytes, rawDocs);
    }

    private void readExact(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int read = channel.read(buf);
            if (read == -1) {
                throw new IOException("Connection closed unexpectedly");
            }
        }
    }

    private static String resolveSocketPath() {
        String envPath = System.getenv("MONGOCORE_BINARY_SOCKET_PATH");
        return envPath != null ? envPath : DEFAULT_SOCKET_PATH;
    }

    private static byte[] encodeBson(BsonDocument doc) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        CODEC.encode(writer, doc, EncoderContext.builder().build());
        writer.close();
        return extractBytes(buffer);
    }

    private static BsonDocument decodeBson(byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        BsonDocument doc = CODEC.decode(reader, DecoderContext.builder().build());
        reader.close();
        return doc;
    }

    private static byte[] extractBytes(BasicOutputBuffer buffer) {
        byte[] bytes = new byte[buffer.getSize()];
        System.arraycopy(buffer.getInternalBuffer(), 0, bytes, 0, buffer.getSize());
        return bytes;
    }

    /**
     * Internal record holding a parsed frame response.
     */
    private record FrameResponse(byte[] envelope, byte[] rawDocs) {}
}
