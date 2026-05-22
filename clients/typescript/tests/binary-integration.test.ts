/**
 * Integration tests for the MongoCore TypeScript binary transport.
 *
 * Requires a running MongoCore sidecar with binary protocol enabled.
 * The binary transport uses a Unix Domain Socket at /tmp/mongocore.bin.sock.
 */

import { BinaryTransport } from '../src/binary-transport';

const BINARY_SOCKET = '/tmp/mongocore.bin.sock';
const TEST_DB = 'mongocore_client_test';

function uniqueCollection(): string {
  return `ts_bin_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

const socketAvailable = BinaryTransport.isAvailable(BINARY_SOCKET);

const describeIfSocket = socketAvailable ? describe : describe.skip;

describeIfSocket('BinaryTransport integration', () => {
  let transport: BinaryTransport;

  beforeAll(async () => {
    transport = new BinaryTransport(BINARY_SOCKET);
    await transport.connect();
  });

  afterAll(() => {
    if (transport) transport.close();
  });

  test('insert and find one', async () => {
    const coll = uniqueCollection();

    const insertedId = await transport.insertOne(TEST_DB, coll, {
      name: 'Alice',
      age: 30,
    });
    expect(insertedId).toBeTruthy();

    const doc = await transport.findOne(TEST_DB, coll, { name: 'Alice' });
    expect(doc).not.toBeNull();
    expect((doc as any).name).toBe('Alice');
    expect((doc as any).age).toBe(30);
  });

  test('insert many', async () => {
    const coll = uniqueCollection();

    const count = await transport.insertMany(TEST_DB, coll, [
      { item: 'a', value: 1 },
      { item: 'b', value: 2 },
      { item: 'c', value: 3 },
    ]);
    expect(count).toBe(3);

    const total = await transport.countDocuments(TEST_DB, coll);
    expect(total).toBe(3);
  });

  test('delete one', async () => {
    const coll = uniqueCollection();

    await transport.insertOne(TEST_DB, coll, { key: 'to_delete' });

    const deleted = await transport.deleteOne(TEST_DB, coll, { key: 'to_delete' });
    expect(deleted).toBe(1);

    const remaining = await transport.countDocuments(TEST_DB, coll);
    expect(remaining).toBe(0);
  });

  test('count documents', async () => {
    const coll = uniqueCollection();

    await transport.insertMany(TEST_DB, coll, [
      { x: 1 },
      { x: 2 },
      { x: 3 },
    ]);

    const count = await transport.countDocuments(TEST_DB, coll);
    expect(count).toBe(3);
  });

  test('count documents with filter', async () => {
    const coll = uniqueCollection();

    await transport.insertMany(TEST_DB, coll, [
      { status: 'active', label: 'one' },
      { status: 'active', label: 'two' },
      { status: 'inactive', label: 'three' },
    ]);

    const activeCount = await transport.countDocuments(TEST_DB, coll, { status: 'active' });
    expect(activeCount).toBe(2);

    const inactiveCount = await transport.countDocuments(TEST_DB, coll, { status: 'inactive' });
    expect(inactiveCount).toBe(1);
  });

  test('find one returns null for no match', async () => {
    const coll = uniqueCollection();

    const doc = await transport.findOne(TEST_DB, coll, { nonexistent: true });
    expect(doc).toBeNull();
  });

  test('update one', async () => {
    const coll = uniqueCollection();

    await transport.insertOne(TEST_DB, coll, { name: 'Bob', score: 10 });

    const result = await transport.updateOne(TEST_DB, coll, { name: 'Bob' }, { $set: { score: 20 } });
    expect(result.matchedCount).toBe(1);
    expect(result.modifiedCount).toBe(1);

    const doc = await transport.findOne(TEST_DB, coll, { name: 'Bob' });
    expect((doc as any).score).toBe(20);
  });

  test('update many', async () => {
    const coll = uniqueCollection();

    await transport.insertMany(TEST_DB, coll, [
      { category: 'a', val: 1 },
      { category: 'a', val: 2 },
      { category: 'b', val: 3 },
    ]);

    const result = await transport.updateMany(TEST_DB, coll, { category: 'a' }, { $set: { val: 99 } });
    expect(result.matchedCount).toBe(2);
    expect(result.modifiedCount).toBe(2);
  });

  test('delete many', async () => {
    const coll = uniqueCollection();

    await transport.insertMany(TEST_DB, coll, [
      { status: 'done' },
      { status: 'done' },
      { status: 'pending' },
    ]);

    const deleted = await transport.deleteMany(TEST_DB, coll, { status: 'done' });
    expect(deleted).toBe(2);

    const remaining = await transport.countDocuments(TEST_DB, coll);
    expect(remaining).toBe(1);
  });

  test('run command ping', async () => {
    const result = await transport.runCommand(TEST_DB, { ping: 1 });
    expect(result).toBeTruthy();
    expect((result as any).ok).toBe(1);
  });
});
