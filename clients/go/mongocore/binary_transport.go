package mongocore

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	headerSize          = 10
	defaultBinarySocket = "/tmp/mongocore.bin.sock"
	defaultMaxFrameSize = 64 * 1024 * 1024

	opFindOne    = 0x02
	opInsertOne  = 0x03
	opInsertMany = 0x04
	opUpdateOne  = 0x05
	opUpdateMany = 0x06
	opDeleteOne  = 0x07
	opDeleteMany = 0x08
	opCount      = 0x0A
	opRunCommand = 0x0D
	opHandshake  = 0x3F
	opError      = 0x3E

	flagEOS     = 1 << 6
	flagNoReply = 1 << 7
)

// BinaryTransportError represents an error from the binary transport layer.
type BinaryTransportError struct {
	Message string
}

func (e *BinaryTransportError) Error() string {
	return fmt.Sprintf("binary transport: %s", e.Message)
}

// BinaryTransport provides high-performance binary communication over Unix domain sockets.
type BinaryTransport struct {
	conn         net.Conn
	reqID        atomic.Uint32
	maxFrameSize uint32
}

// NewBinaryTransport connects to the MongoCore binary UDS and performs a handshake.
// Socket path resolution: MONGOCORE_BINARY_SOCKET_PATH env > socketPath parameter > default.
func NewBinaryTransport(socketPath string) (*BinaryTransport, error) {
	path := resolveBinarySocketPath(socketPath)

	conn, err := net.Dial("unix", path)
	if err != nil {
		return nil, &BinaryTransportError{Message: fmt.Sprintf("failed to connect to %s: %v", path, err)}
	}

	bt := &BinaryTransport{
		conn:         conn,
		maxFrameSize: defaultMaxFrameSize,
	}

	if err := bt.handshake(); err != nil {
		conn.Close()
		return nil, err
	}

	return bt, nil
}

// Close closes the underlying connection.
func (bt *BinaryTransport) Close() error {
	if bt.conn != nil {
		return bt.conn.Close()
	}
	return nil
}

// FindOne finds a single document matching the filter.
func (bt *BinaryTransport) FindOne(db, collection string, filter bson.M) (bson.M, error) {
	envelope := bson.M{"db": db, "coll": collection, "filter": filter}

	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return nil, &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opFindOne, envelopeBytes, nil, false); err != nil {
		return nil, err
	}

	respEnvelope, rawDocs, err := bt.readFrame()
	if err != nil {
		return nil, err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return nil, &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return nil, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	if len(rawDocs) == 0 {
		return nil, nil
	}

	var doc bson.M
	if err := bson.Unmarshal(rawDocs, &doc); err != nil {
		return nil, &BinaryTransportError{Message: fmt.Sprintf("unmarshal document: %v", err)}
	}
	return doc, nil
}

// InsertOne inserts a single document and returns the inserted ID as a string.
func (bt *BinaryTransport) InsertOne(db, collection string, document bson.M) (string, error) {
	rawDoc, err := bson.Marshal(document)
	if err != nil {
		return "", &BinaryTransportError{Message: fmt.Sprintf("marshal document: %v", err)}
	}

	envelope := bson.M{"db": db, "coll": collection}
	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return "", &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opInsertOne, envelopeBytes, rawDoc, false); err != nil {
		return "", err
	}

	respEnvelope, _, err := bt.readFrame()
	if err != nil {
		return "", err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return "", &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return "", &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	insertedID := fmt.Sprintf("%v", resp["inserted_id"])
	return insertedID, nil
}

// DeleteOne deletes a single document matching the filter and returns the deleted count.
func (bt *BinaryTransport) DeleteOne(db, collection string, filter bson.M) (int64, error) {
	envelope := bson.M{"db": db, "coll": collection, "filter": filter}

	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opDeleteOne, envelopeBytes, nil, false); err != nil {
		return 0, err
	}

	respEnvelope, _, err := bt.readFrame()
	if err != nil {
		return 0, err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	count, _ := resp["deleted_count"].(int64)
	if count == 0 {
		// Try int32 fallback
		if c32, ok := resp["deleted_count"].(int32); ok {
			count = int64(c32)
		}
	}
	return count, nil
}

// CountDocuments counts documents matching the filter.
func (bt *BinaryTransport) CountDocuments(db, collection string, filter bson.M) (int64, error) {
	if filter == nil {
		filter = bson.M{}
	}

	envelope := bson.M{"db": db, "coll": collection, "filter": filter}

	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opCount, envelopeBytes, nil, false); err != nil {
		return 0, err
	}

	respEnvelope, _, err := bt.readFrame()
	if err != nil {
		return 0, err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	count, _ := resp["count"].(int64)
	if count == 0 {
		if c32, ok := resp["count"].(int32); ok {
			count = int64(c32)
		}
	}
	return count, nil
}

// InsertMany inserts multiple documents and returns the number of documents inserted.
func (bt *BinaryTransport) InsertMany(db, collection string, documents []bson.M, ordered bool) (int, error) {
	var rawDocs []byte
	for _, doc := range documents {
		raw, err := bson.Marshal(doc)
		if err != nil {
			return 0, &BinaryTransportError{Message: fmt.Sprintf("marshal document: %v", err)}
		}
		rawDocs = append(rawDocs, raw...)
	}

	envelope := bson.M{"db": db, "coll": collection, "ordered": ordered, "count": len(documents)}
	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opInsertMany, envelopeBytes, rawDocs, false); err != nil {
		return 0, err
	}

	respEnvelope, _, err := bt.readFrame()
	if err != nil {
		return 0, err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	count, _ := resp["inserted_count"].(int32)
	return int(count), nil
}

// UpdateOne updates a single document matching the filter and returns (matchedCount, modifiedCount).
func (bt *BinaryTransport) UpdateOne(db, collection string, filter, update bson.M) (int64, int64, error) {
	envelope := bson.M{"db": db, "coll": collection, "filter": filter, "update": update}

	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return 0, 0, &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opUpdateOne, envelopeBytes, nil, false); err != nil {
		return 0, 0, err
	}

	respEnvelope, _, err := bt.readFrame()
	if err != nil {
		return 0, 0, err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return 0, 0, &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return 0, 0, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	matched := toInt64(resp["matched_count"])
	modified := toInt64(resp["modified_count"])
	return matched, modified, nil
}

// UpdateMany updates all documents matching the filter and returns (matchedCount, modifiedCount).
func (bt *BinaryTransport) UpdateMany(db, collection string, filter, update bson.M) (int64, int64, error) {
	envelope := bson.M{"db": db, "coll": collection, "filter": filter, "update": update}

	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return 0, 0, &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opUpdateMany, envelopeBytes, nil, false); err != nil {
		return 0, 0, err
	}

	respEnvelope, _, err := bt.readFrame()
	if err != nil {
		return 0, 0, err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return 0, 0, &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return 0, 0, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	matched := toInt64(resp["matched_count"])
	modified := toInt64(resp["modified_count"])
	return matched, modified, nil
}

// DeleteMany deletes all documents matching the filter and returns the deleted count.
func (bt *BinaryTransport) DeleteMany(db, collection string, filter bson.M) (int64, error) {
	envelope := bson.M{"db": db, "coll": collection, "filter": filter}

	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opDeleteMany, envelopeBytes, nil, false); err != nil {
		return 0, err
	}

	respEnvelope, _, err := bt.readFrame()
	if err != nil {
		return 0, err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	count := toInt64(resp["deleted_count"])
	return count, nil
}

// RunCommand runs an arbitrary database command and returns the result document.
func (bt *BinaryTransport) RunCommand(db string, command bson.M) (bson.M, error) {
	envelope := bson.M{"db": db, "command": command}
	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return nil, &BinaryTransportError{Message: fmt.Sprintf("marshal envelope: %v", err)}
	}

	if _, err := bt.sendFrame(opRunCommand, envelopeBytes, nil, false); err != nil {
		return nil, err
	}

	respEnvelope, rawDocs, err := bt.readFrame()
	if err != nil {
		return nil, err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return nil, &BinaryTransportError{Message: fmt.Sprintf("unmarshal response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return nil, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
	}

	if len(rawDocs) > 0 {
		var doc bson.M
		if err := bson.Unmarshal(rawDocs, &doc); err != nil {
			return nil, &BinaryTransportError{Message: fmt.Sprintf("unmarshal result: %v", err)}
		}
		return doc, nil
	}
	return resp, nil
}

// toInt64 converts a BSON numeric value to int64.
func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// BinaryTransportAvailable checks if the binary transport socket exists.
func BinaryTransportAvailable(socketPath string) bool {
	path := resolveBinarySocketPath(socketPath)
	_, err := os.Stat(path)
	return err == nil
}

// --- Internal methods ---

func (bt *BinaryTransport) handshake() error {
	envelope := bson.M{"client_language": "go"}
	envelopeBytes, err := bson.Marshal(envelope)
	if err != nil {
		return &BinaryTransportError{Message: fmt.Sprintf("marshal handshake: %v", err)}
	}

	if _, err := bt.sendFrame(opHandshake, envelopeBytes, nil, false); err != nil {
		return err
	}

	respEnvelope, _, err := bt.readFrame()
	if err != nil {
		return err
	}

	var resp bson.M
	if err := bson.Unmarshal(respEnvelope, &resp); err != nil {
		return &BinaryTransportError{Message: fmt.Sprintf("unmarshal handshake response: %v", err)}
	}

	if errMsg, ok := resp["error"]; ok {
		return &BinaryTransportError{Message: fmt.Sprintf("handshake failed: %v", errMsg)}
	}

	return nil
}

func (bt *BinaryTransport) sendFrame(opcode uint8, envelope, rawDocs []byte, noReply bool) (uint32, error) {
	reqID := bt.reqID.Add(1)

	// flags: bits[0:5] = opcode, bit6 = EOS (always set for single frame), bit7 = no-reply
	flags := uint16(opcode&0x3F) | flagEOS
	if noReply {
		flags |= flagNoReply
	}

	// msg_len = flags(2) + req_id(4) + len(envelope) + len(rawDocs)
	msgLen := uint32(2 + 4 + len(envelope) + len(rawDocs))

	// Header: 4B msg_len (BE) + 2B flags (BE) + 4B req_id (BE) = 10 bytes
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[0:4], msgLen)
	binary.BigEndian.PutUint16(header[4:6], flags)
	binary.BigEndian.PutUint32(header[6:10], reqID)

	// Write header + envelope + rawDocs
	buf := make([]byte, 0, headerSize+len(envelope)+len(rawDocs))
	buf = append(buf, header...)
	buf = append(buf, envelope...)
	buf = append(buf, rawDocs...)

	if _, err := bt.conn.Write(buf); err != nil {
		return 0, &BinaryTransportError{Message: fmt.Sprintf("write frame: %v", err)}
	}

	return reqID, nil
}

func (bt *BinaryTransport) readFrame() (envelope, rawDocs []byte, err error) {
	// Read header
	header := make([]byte, headerSize)
	if err := bt.readExact(header); err != nil {
		return nil, nil, err
	}

	msgLen := binary.BigEndian.Uint32(header[0:4])
	flags := binary.BigEndian.Uint16(header[4:6])
	// req_id at header[6:10] — not needed for response processing

	// payload_len = msg_len - flags(2) - req_id(4)
	payloadLen := int(msgLen) - 2 - 4
	if payloadLen < 0 {
		return nil, nil, &BinaryTransportError{Message: fmt.Sprintf("invalid frame: msg_len=%d too small", msgLen)}
	}
	if uint32(payloadLen) > bt.maxFrameSize {
		return nil, nil, &BinaryTransportError{Message: fmt.Sprintf("frame too large: %d bytes", payloadLen)}
	}

	opcode := flags & 0x3F
	if opcode == opError {
		if payloadLen > 0 {
			payload := make([]byte, payloadLen)
			if err := bt.readExact(payload); err != nil {
				return nil, nil, err
			}
			var errDoc bson.M
			if unmarshalErr := bson.Unmarshal(payload, &errDoc); unmarshalErr == nil {
				if errMsg, ok := errDoc["error"]; ok {
					return nil, nil, &BinaryTransportError{Message: fmt.Sprintf("%v", errMsg)}
				}
			}
		}
		return nil, nil, &BinaryTransportError{Message: "unknown server error"}
	}

	if payloadLen == 0 {
		return []byte{}, []byte{}, nil
	}

	payload := make([]byte, payloadLen)
	if err := bt.readExact(payload); err != nil {
		return nil, nil, err
	}

	// Split envelope from raw docs using BSON self-delimiting length prefix (LE i32)
	if len(payload) < 4 {
		return payload, []byte{}, nil
	}

	envelopeLen := int(binary.LittleEndian.Uint32(payload[0:4]))
	if envelopeLen < 5 || envelopeLen > len(payload) {
		return nil, nil, &BinaryTransportError{
			Message: fmt.Sprintf("invalid envelope length: %d (payload=%d)", envelopeLen, len(payload)),
		}
	}

	return payload[:envelopeLen], payload[envelopeLen:], nil
}

func (bt *BinaryTransport) readExact(buf []byte) error {
	total := 0
	for total < len(buf) {
		n, err := bt.conn.Read(buf[total:])
		if err != nil {
			return &BinaryTransportError{Message: fmt.Sprintf("read error (got %d/%d bytes): %v", total, len(buf), err)}
		}
		total += n
	}
	return nil
}

func resolveBinarySocketPath(socketPath string) string {
	if envPath := os.Getenv("MONGOCORE_BINARY_SOCKET_PATH"); envPath != "" {
		return envPath
	}
	if socketPath != "" {
		return socketPath
	}
	return defaultBinarySocket
}

// BinaryTransportPool maintains a pool of BinaryTransport connections and
// distributes requests using round-robin selection. Default pool size of 4
// connections supports high-throughput CPU-bound workloads.
type BinaryTransportPool struct {
	connections []*BinaryTransport
	index       atomic.Uint32
}

// NewBinaryTransportPool creates a pool of binary transport connections.
// If poolSize <= 0, defaults to 4. On any connection failure, all already-opened
// connections are closed and the error is returned.
func NewBinaryTransportPool(socketPath string, poolSize int) (*BinaryTransportPool, error) {
	if poolSize <= 0 {
		poolSize = 4
	}
	pool := &BinaryTransportPool{
		connections: make([]*BinaryTransport, 0, poolSize),
	}
	for i := 0; i < poolSize; i++ {
		conn, err := NewBinaryTransport(socketPath)
		if err != nil {
			pool.Close()
			return nil, err
		}
		pool.connections = append(pool.connections, conn)
	}
	return pool, nil
}

// Close closes all connections in the pool.
func (p *BinaryTransportPool) Close() error {
	var lastErr error
	for _, conn := range p.connections {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
	}
	p.connections = nil
	return lastErr
}

// next returns the next connection using atomic round-robin selection.
func (p *BinaryTransportPool) next() *BinaryTransport {
	idx := p.index.Add(1) - 1
	return p.connections[idx%uint32(len(p.connections))]
}

// FindOne finds a single document matching the filter.
func (p *BinaryTransportPool) FindOne(db, collection string, filter bson.M) (bson.M, error) {
	return p.next().FindOne(db, collection, filter)
}

// InsertOne inserts a single document and returns the inserted ID as a string.
func (p *BinaryTransportPool) InsertOne(db, collection string, document bson.M) (string, error) {
	return p.next().InsertOne(db, collection, document)
}

// DeleteOne deletes a single document matching the filter and returns the deleted count.
func (p *BinaryTransportPool) DeleteOne(db, collection string, filter bson.M) (int64, error) {
	return p.next().DeleteOne(db, collection, filter)
}

// CountDocuments counts documents matching the filter.
func (p *BinaryTransportPool) CountDocuments(db, collection string, filter bson.M) (int64, error) {
	return p.next().CountDocuments(db, collection, filter)
}

// InsertMany inserts multiple documents and returns the number of documents inserted.
func (p *BinaryTransportPool) InsertMany(db, collection string, documents []bson.M, ordered bool) (int, error) {
	return p.next().InsertMany(db, collection, documents, ordered)
}

// UpdateOne updates a single document matching the filter.
func (p *BinaryTransportPool) UpdateOne(db, collection string, filter, update bson.M) (int64, int64, error) {
	return p.next().UpdateOne(db, collection, filter, update)
}

// UpdateMany updates all documents matching the filter.
func (p *BinaryTransportPool) UpdateMany(db, collection string, filter, update bson.M) (int64, int64, error) {
	return p.next().UpdateMany(db, collection, filter, update)
}

// DeleteMany deletes all documents matching the filter and returns the deleted count.
func (p *BinaryTransportPool) DeleteMany(db, collection string, filter bson.M) (int64, error) {
	return p.next().DeleteMany(db, collection, filter)
}

// RunCommand runs an arbitrary database command and returns the result document.
func (p *BinaryTransportPool) RunCommand(db string, command bson.M) (bson.M, error) {
	return p.next().RunCommand(db, command)
}
