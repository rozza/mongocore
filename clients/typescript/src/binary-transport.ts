import * as net from 'net';
import * as fs from 'fs';
import { BSON } from 'bson';

const HEADER_SIZE = 10;
const DEFAULT_SOCKET_PATH = '/tmp/mongocore.bin.sock';
const DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

// Opcodes
const OP_FIND_ONE = 0x02;
const OP_INSERT_ONE = 0x03;
const OP_INSERT_MANY = 0x04;
const OP_UPDATE_ONE = 0x05;
const OP_UPDATE_MANY = 0x06;
const OP_DELETE_ONE = 0x07;
const OP_DELETE_MANY = 0x08;
const OP_COUNT = 0x0A;
const OP_RUN_COMMAND = 0x0D;
const OP_HANDSHAKE = 0x3F;

// Flag bits
const FLAG_EOS = 1 << 6;
const FLAG_NO_REPLY = 1 << 7;

/**
 * Binary UDS transport client for MongoCore.
 *
 * Connects to MongoCore's binary protocol over Unix Domain Socket for
 * low-latency, high-throughput communication using BSON-encoded frames.
 */
export class BinaryTransport {
  private socketPath: string;
  private socket: net.Socket | null = null;
  private maxFrameSize: number = DEFAULT_MAX_FRAME_SIZE;
  private reqId: number = 0;
  private buffer: Buffer = Buffer.alloc(0);
  private dataResolve: ((value: void) => void) | null = null;

  constructor(socketPath?: string) {
    this.socketPath = socketPath
      || process.env.MONGOCORE_BINARY_SOCKET_PATH
      || DEFAULT_SOCKET_PATH;
  }

  /**
   * Connect to the MongoCore binary UDS socket and perform handshake.
   */
  async connect(): Promise<void> {
    await this.connectSocket();
    await this.handshake();
  }

  /**
   * Close the connection.
   */
  close(): void {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.buffer = Buffer.alloc(0);
  }

  /**
   * Find a single document matching the filter.
   */
  async findOne(
    db: string,
    collection: string,
    filter: object,
    projection?: object,
  ): Promise<object | null> {
    const envelope: Record<string, unknown> = { db, coll: collection, filter };
    if (projection) {
      envelope.projection = projection;
    }

    const reqId = await this.sendFrame(OP_FIND_ONE, envelope);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    if (response.rawDocs.length === 0) {
      return null;
    }

    return BSON.deserialize(response.rawDocs) as object;
  }

  /**
   * Insert a single document. Returns the inserted ID as a string.
   */
  async insertOne(db: string, collection: string, document: object): Promise<string> {
    const envelope = { db, coll: collection };
    const rawDocs = Buffer.from(BSON.serialize(document));

    const reqId = await this.sendFrame(OP_INSERT_ONE, envelope, rawDocs);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    return result.inserted_id as string;
  }

  /**
   * Insert multiple documents. Returns the count of inserted documents.
   */
  async insertMany(
    db: string,
    collection: string,
    documents: object[],
    ordered?: boolean,
  ): Promise<number> {
    const envelope: Record<string, unknown> = { db, coll: collection };
    if (ordered !== undefined) {
      envelope.ordered = ordered;
    }

    // Concatenate BSON documents into raw docs buffer
    const parts = documents.map(doc => Buffer.from(BSON.serialize(doc)));
    const rawDocs = Buffer.concat(parts);

    const reqId = await this.sendFrame(OP_INSERT_MANY, envelope, rawDocs);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    return result.inserted_count as number;
  }

  /**
   * Delete a single document matching the filter. Returns the deleted count.
   */
  async deleteOne(db: string, collection: string, filter: object): Promise<number> {
    const envelope = { db, coll: collection, filter };

    const reqId = await this.sendFrame(OP_DELETE_ONE, envelope);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    return result.deleted_count as number;
  }

  /**
   * Count documents matching the filter.
   */
  async countDocuments(db: string, collection: string, filter?: object): Promise<number> {
    const envelope: Record<string, unknown> = { db, coll: collection };
    if (filter) {
      envelope.filter = filter;
    }

    const reqId = await this.sendFrame(OP_COUNT, envelope);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    return result.count as number;
  }

  /**
   * Update a single document matching the filter.
   * Returns matched and modified counts.
   */
  async updateOne(
    db: string,
    collection: string,
    filter: object,
    update: object,
  ): Promise<{ matchedCount: number; modifiedCount: number }> {
    const envelope = { db, coll: collection, filter, update, doc_bytes_len: 0 };

    await this.sendFrame(OP_UPDATE_ONE, envelope);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    return {
      matchedCount: result.matched_count as number,
      modifiedCount: result.modified_count as number,
    };
  }

  /**
   * Update multiple documents matching the filter.
   * Returns matched and modified counts.
   */
  async updateMany(
    db: string,
    collection: string,
    filter: object,
    update: object,
  ): Promise<{ matchedCount: number; modifiedCount: number }> {
    const envelope = { db, coll: collection, filter, update, doc_bytes_len: 0 };

    await this.sendFrame(OP_UPDATE_MANY, envelope);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    return {
      matchedCount: result.matched_count as number,
      modifiedCount: result.modified_count as number,
    };
  }

  /**
   * Delete multiple documents matching the filter. Returns the deleted count.
   */
  async deleteMany(db: string, collection: string, filter: object): Promise<number> {
    const envelope = { db, coll: collection, filter, doc_bytes_len: 0 };

    await this.sendFrame(OP_DELETE_MANY, envelope);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    return result.deleted_count as number;
  }

  /**
   * Run a raw database command. Returns the result document.
   */
  async runCommand(db: string, command: object): Promise<object> {
    const envelope = { db, command, doc_bytes_len: 0 };

    await this.sendFrame(OP_RUN_COMMAND, envelope);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(result.error as string);
    }

    if (response.rawDocs.length > 0) {
      return BSON.deserialize(response.rawDocs) as object;
    }

    return result as object;
  }

  /**
   * Check if the binary transport socket is available.
   */
  static isAvailable(socketPath?: string): boolean {
    const path = socketPath
      || process.env.MONGOCORE_BINARY_SOCKET_PATH
      || DEFAULT_SOCKET_PATH;
    try {
      fs.accessSync(path);
      return true;
    } catch {
      return false;
    }
  }

  // --- Private methods ---

  private async connectSocket(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection(this.socketPath, () => {
        resolve();
      });

      this.socket.on('error', (err) => {
        reject(new Error(`Failed to connect to ${this.socketPath}: ${err.message}`));
      });

      this.socket.on('data', (chunk: Buffer) => {
        this.buffer = Buffer.concat([this.buffer, chunk]);
        if (this.dataResolve) {
          const resolve = this.dataResolve;
          this.dataResolve = null;
          resolve();
        }
      });
    });
  }

  private async handshake(): Promise<void> {
    const envelope = {
      client_language: 'typescript',
    };

    await this.sendFrame(OP_HANDSHAKE, envelope);
    const response = await this.readFrame();
    const result = BSON.deserialize(response.envelope);

    if (result.error) {
      throw new Error(`Handshake failed: ${result.error}`);
    }

    if (result.max_frame_size) {
      this.maxFrameSize = result.max_frame_size as number;
    }
  }

  private nextReqId(): number {
    this.reqId = (this.reqId + 1) & 0xFFFFFFFF;
    return this.reqId;
  }

  private async sendFrame(
    opcode: number,
    envelope: object,
    rawDocs?: Buffer,
    noReply?: boolean,
  ): Promise<number> {
    if (!this.socket) {
      throw new Error('Not connected');
    }

    const envelopeBuf = Buffer.from(BSON.serialize(envelope));
    const docsBuf = rawDocs || Buffer.alloc(0);
    const reqId = this.nextReqId();

    // msg_len = flags(2) + req_id(4) + envelope + rawDocs
    const msgLen = 2 + 4 + envelopeBuf.length + docsBuf.length;

    if (msgLen > this.maxFrameSize) {
      throw new Error(`Frame size ${msgLen} exceeds max frame size ${this.maxFrameSize}`);
    }

    // Build flags: bits[0:5] = opcode, bit6 = EOS, bit7 = no-reply
    let flags = (opcode & 0x3F) | FLAG_EOS;
    if (noReply) {
      flags |= FLAG_NO_REPLY;
    }

    // Header: 4B msg_len (BE) + 2B flags (BE) + 4B req_id (BE)
    const header = Buffer.alloc(HEADER_SIZE);
    header.writeUInt32BE(msgLen, 0);
    header.writeUInt16BE(flags, 4);
    header.writeUInt32BE(reqId, 6);

    const frame = Buffer.concat([header, envelopeBuf, docsBuf]);

    return new Promise((resolve, reject) => {
      this.socket!.write(frame, (err) => {
        if (err) return reject(err);
        resolve(reqId);
      });
    });
  }

  private async readFrame(): Promise<{ envelope: Buffer; rawDocs: Buffer }> {
    // Read header
    const header = await this.readExact(HEADER_SIZE);
    const msgLen = header.readUInt32BE(0);
    const flags = header.readUInt16BE(4);

    if (msgLen > this.maxFrameSize) {
      throw new Error(`Response frame size ${msgLen} exceeds max frame size ${this.maxFrameSize}`);
    }

    // Payload size = msgLen - flags(2) - req_id(4)
    const payloadSize = msgLen - 6;
    const payload = await this.readExact(payloadSize);

    // Check for error opcode (0x3E)
    const opcode = flags & 0x3F;
    if (opcode === 0x3E) {
      const envelopeSize = payload.readInt32LE(0);
      const errDoc = BSON.deserialize(payload.subarray(0, envelopeSize));
      throw new Error(errDoc.message || errDoc.error || 'Unknown server error');
    }

    // First 4 bytes of payload are LE i32 = BSON envelope size
    const envelopeSize = payload.readInt32LE(0);
    const envelope = payload.subarray(0, envelopeSize);
    const rawDocs = payload.subarray(envelopeSize);

    return { envelope, rawDocs };
  }

  private async readExact(n: number): Promise<Buffer> {
    while (this.buffer.length < n) {
      await this.waitForData();
    }

    const result = this.buffer.subarray(0, n);
    this.buffer = this.buffer.subarray(n);
    return result;
  }

  private waitForData(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.socket) {
        return reject(new Error('Socket closed'));
      }

      this.dataResolve = resolve;

      // Handle socket close/error while waiting
      const onClose = () => {
        this.dataResolve = null;
        reject(new Error('Socket closed while reading'));
      };
      const onError = (err: Error) => {
        this.dataResolve = null;
        reject(err);
      };

      this.socket.once('close', onClose);
      this.socket.once('error', onError);

      // Clean up listeners when resolved
      const originalResolve = this.dataResolve;
      this.dataResolve = () => {
        this.socket?.removeListener('close', onClose);
        this.socket?.removeListener('error', onError);
        resolve();
      };
    });
  }
}

/**
 * Connection pool for BinaryTransport with round-robin dispatch.
 *
 * Creates multiple connections to MongoCore's binary protocol and distributes
 * requests across them for improved throughput under concurrent workloads.
 */
export class BinaryTransportPool {
  private connections: BinaryTransport[] = [];
  private index = 0;
  private poolSize: number;
  private socketPath?: string;

  constructor(socketPath?: string, poolSize = 4) {
    this.socketPath = socketPath;
    this.poolSize = poolSize;
  }

  /**
   * Connect all pool members to the MongoCore binary UDS socket.
   */
  async connect(): Promise<void> {
    for (let i = 0; i < this.poolSize; i++) {
      const conn = new BinaryTransport(this.socketPath);
      await conn.connect();
      this.connections.push(conn);
    }
  }

  /**
   * Close all connections in the pool.
   */
  close(): void {
    for (const conn of this.connections) {
      conn.close();
    }
    this.connections = [];
  }

  private next(): BinaryTransport {
    const conn = this.connections[this.index % this.connections.length];
    this.index++;
    return conn;
  }

  // Proxy all methods via round-robin dispatch
  async findOne(...args: Parameters<BinaryTransport['findOne']>) { return this.next().findOne(...args); }
  async insertOne(...args: Parameters<BinaryTransport['insertOne']>) { return this.next().insertOne(...args); }
  async insertMany(...args: Parameters<BinaryTransport['insertMany']>) { return this.next().insertMany(...args); }
  async updateOne(...args: Parameters<BinaryTransport['updateOne']>) { return this.next().updateOne(...args); }
  async updateMany(...args: Parameters<BinaryTransport['updateMany']>) { return this.next().updateMany(...args); }
  async deleteOne(...args: Parameters<BinaryTransport['deleteOne']>) { return this.next().deleteOne(...args); }
  async deleteMany(...args: Parameters<BinaryTransport['deleteMany']>) { return this.next().deleteMany(...args); }
  async countDocuments(...args: Parameters<BinaryTransport['countDocuments']>) { return this.next().countDocuments(...args); }
  async runCommand(...args: Parameters<BinaryTransport['runCommand']>) { return this.next().runCommand(...args); }
}
