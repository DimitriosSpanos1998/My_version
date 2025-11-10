const MongoService = require('../src/services/mongo-service');
const { createMockDatabaseConfig } = require('./helpers/mock-database-config');

describe('MongoService Integration Tests', () => {
  let mongoService;
  let mockConfig;

  const clearCollections = async () => {
    if (!mongoService || !mongoService.isDatabaseConnected()) {
      return;
    }

    const normalized = mongoService.getCollection('normalized');
    const original = mongoService.getCollection('original');

    await Promise.all([
      normalized.deleteMany({}),
      original.deleteMany({})
    ]);
  };

  beforeAll(async () => {
    mockConfig = createMockDatabaseConfig();
    mongoService = new MongoService(mockConfig);
    await mongoService.connect();
  });

  afterAll(async () => {
    await clearCollections();
    await mongoService.close();
  });

  beforeEach(async () => {
    await clearCollections();
  });

  describe('CRUD Operations', () => {
    test('should perform complete CRUD cycle', async () => {
      const testDoc = {
        summary: {
          title: 'CRUD Test API',
          version: '1.0.0',
          description: 'API for testing CRUD operations',
          protocol: 'websocket',
          channelsCount: 3,
          serversCount: 2,
          createdAt: new Date(),
          updatedAt: new Date(),
          processedAt: new Date()
        },
        searchableFields: {
          title: 'crud test api',
          description: 'api for testing crud operations',
          version: '1.0.0',
          protocol: 'websocket',
          tags: ['test', 'crud']
        }
      };

      const insertResult = await mongoService.insertAsyncAPIDocument({
        original: JSON.stringify({ title: testDoc.summary.title }),
        normalized: testDoc
      });
      expect(insertResult.success).toBe(true);
      const docId = insertResult.insertedId.toString();
      expect(insertResult.originalId).toBeDefined();

      const originalCollection = mongoService.getCollection('original');
      const storedOriginal = await originalCollection.findOne({ normalizedId: insertResult.insertedId });
      expect(storedOriginal).toBeDefined();
      expect(storedOriginal.normalizedId.toString()).toBe(docId);
      expect(storedOriginal.raw).toContain('CRUD Test API');
      expect(storedOriginal.metadata).toBeUndefined();

      const foundDoc = await mongoService.findAsyncAPIDocumentById(docId);
      expect(foundDoc).toBeDefined();
      expect(foundDoc.summary.title).toBe('CRUD Test API');

      const updateResult = await mongoService.updateAsyncAPIDocument(docId, {
        'summary.description': 'Updated description',
        'summary.version': '1.1.0'
      });
      expect(updateResult.success).toBe(true);
      expect(updateResult.modifiedCount).toBe(1);

      const updatedDoc = await mongoService.findAsyncAPIDocumentById(docId);
      expect(updatedDoc.summary.description).toBe('Updated description');
      expect(updatedDoc.summary.version).toBe('1.1.0');

      const deleteResult = await mongoService.deleteAsyncAPIDocument(docId);
      expect(deleteResult.success).toBe(true);
      expect(deleteResult.deletedCount).toBe(1);

      const deletedDoc = await mongoService.findAsyncAPIDocumentById(docId);
      expect(deletedDoc).toBeNull();

      const remainingOriginal = await originalCollection.countDocuments({ normalizedId: insertResult.insertedId });
      expect(remainingOriginal).toBe(0);
    });

    test('should preserve original AsyncAPI specification in original collection', async () => {
      const now = new Date();
      const asyncAPISpec = {
        asyncapi: '2.6.0',
        info: {
          title: 'Banking Transactions API',
          version: '1.0.0',
          description: 'Streams transaction events for a retail banking platform.'
        },
        servers: {
          production: {
            url: 'mqtts://broker.bank.example.com:8883',
            protocol: 'mqtt',
            description: 'Secure production broker.'
          }
        },
        defaultContentType: 'application/json',
        channels: {
          'transaction/created': {
            description: 'Notifies when new transactions are recorded.',
            subscribe: {
              summary: 'Receive newly created transaction events.',
              message: {
                name: 'TransactionCreated',
                payload: {
                  type: 'object',
                  properties: {
                    transactionId: { type: 'string' },
                    accountId: { type: 'string' },
                    amount: { type: 'number' },
                    currency: { type: 'string' },
                    timestamp: {
                      type: 'string',
                      format: 'date-time'
                    }
                  }
                }
              }
            }
          }
        }
      };

      const summary = {
        title: asyncAPISpec.info.title,
        version: asyncAPISpec.info.version,
        description: asyncAPISpec.info.description,
        protocol: 'mqtt',
        channelsCount: Object.keys(asyncAPISpec.channels).length,
        serversCount: Object.keys(asyncAPISpec.servers).length,
        tags: [],
        createdAt: now,
        updatedAt: now,
        processedAt: now
      };

      const searchableFields = {
        title: summary.title.toLowerCase(),
        description: summary.description.toLowerCase(),
        version: summary.version,
        protocol: summary.protocol,
        tags: []
      };

      const insertResult = await mongoService.insertAsyncAPIDocument({
        original: JSON.stringify(asyncAPISpec, null, 2),
        normalized: {
          summary,
          searchableFields
        },
        summary,
        searchableFields
      });

      expect(insertResult.success).toBe(true);

      const originalCollection = mongoService.getCollection('original');
      const storedOriginal = await originalCollection.findOne({ normalizedId: insertResult.insertedId });

      expect(storedOriginal).toBeDefined();
      expect(storedOriginal.raw).toEqual(JSON.stringify(asyncAPISpec, null, 2));
    });
  });

  describe('Query Operations', () => {
    beforeEach(async () => {
      const testDocs = [
        {
          summary: {
            title: 'MQTT API',
            version: '1.0.0',
            protocol: 'mqtt',
            channelsCount: 5,
            serversCount: 1,
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
          searchableFields: {
            title: 'mqtt api',
            description: 'mqtt protocol api',
            version: '1.0.0',
            protocol: 'mqtt',
            tags: ['mqtt', 'iot']
          }
        },
        {
          summary: {
            title: 'WebSocket API',
            version: '2.0.0',
            protocol: 'ws',
            channelsCount: 3,
            serversCount: 2,
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
          searchableFields: {
            title: 'websocket api',
            description: 'websocket protocol api',
            version: '2.0.0',
            protocol: 'ws',
            tags: ['websocket', 'realtime']
          }
        }
      ];

      for (const doc of testDocs) {
        await mongoService.insertAsyncAPIDocument({
          original: JSON.stringify({ title: doc.summary.title }),
          normalized: doc
        });
      }
    });

    test('should find documents by protocol', async () => {
      const mqttDocs = await mongoService.findDocumentsByProtocol('mqtt');
      expect(mqttDocs).toHaveLength(1);
      expect(mqttDocs[0].summary.protocol).toBe('mqtt');

      const wsDocs = await mongoService.findDocumentsByProtocol('ws');
      expect(wsDocs).toHaveLength(1);
      expect(wsDocs[0].summary.protocol).toBe('ws');
    });

    test('should find documents by version', async () => {
      const v1Docs = await mongoService.findDocumentsByVersion('1.0.0');
      expect(v1Docs).toHaveLength(1);
      expect(v1Docs[0].summary.version).toBe('1.0.0');

      const v2Docs = await mongoService.findDocumentsByVersion('2.0.0');
      expect(v2Docs).toHaveLength(1);
      expect(v2Docs[0].summary.version).toBe('2.0.0');
    });

    test('should search documents by text', async () => {
      const mqttResults = await mongoService.searchAsyncAPIDocuments('mqtt');
      expect(mqttResults).toHaveLength(1);
      expect(mqttResults[0].summary.title).toBe('MQTT API');

      const wsResults = await mongoService.searchAsyncAPIDocuments('websocket');
      expect(wsResults).toHaveLength(1);
      expect(wsResults[0].summary.title).toBe('WebSocket API');
    });

    test('should find documents with complex queries', async () => {

    test('should find documents by protocol', async () => {
      const mqttDocs = await mongoService.findDocumentsByProtocol('mqtt');
      expect(mqttDocs).toHaveLength(1);
      expect(mqttDocs[0].summary.protocol).toBe('mqtt');

      const wsDocs = await mongoService.findDocumentsByProtocol('ws');
      expect(wsDocs).toHaveLength(1);
      expect(wsDocs[0].summary.protocol).toBe('ws');
    });

    test('should find documents by version', async () => {
      const v1Docs = await mongoService.findDocumentsByVersion('1.0.0');
      expect(v1Docs).toHaveLength(1);
      expect(v1Docs[0].summary.version).toBe('1.0.0');

      const v2Docs = await mongoService.findDocumentsByVersion('2.0.0');
      expect(v2Docs).toHaveLength(1);
      expect(v2Docs[0].summary.version).toBe('2.0.0');
    });

    test('should search documents by text', async () => {
      const mqttResults = await mongoService.searchAsyncAPIDocuments('mqtt');
      expect(mqttResults).toHaveLength(1);
      expect(mqttResults[0].summary.title).toBe('MQTT API');

      const wsResults = await mongoService.searchAsyncAPIDocuments('websocket');
      expect(wsResults).toHaveLength(1);
      expect(wsResults[0].summary.title).toBe('WebSocket API');
    });

    test('should find documents with complex queries', async () => {
      const multiChannelDocs = await mongoService.findAsyncAPIDocuments({
        'summary.channelsCount': { $gt: 3 }
      });
      expect(multiChannelDocs).toHaveLength(1);
      expect(multiChannelDocs[0].summary.title).toBe('MQTT API');

      const multiServerDocs = await mongoService.findAsyncAPIDocuments({
        'summary.serversCount': { $gte: 2 }
      });
      expect(multiServerDocs).toHaveLength(1);
      expect(multiServerDocs[0].summary.title).toBe('WebSocket API');
    });
  });

  describe('Statistics and Aggregations', () => {
    beforeEach(async () => {
    });
  });

  describe('Statistics and Aggregations', () => {
    beforeEach(async () => {
      // Insert diverse test data
      const testDocs = [
        {
          summary: {
            title: 'API 1',
            version: '1.0.0',
            protocol: 'mqtt',
            channelsCount: 2,
            serversCount: 1,
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
          searchableFields: {
            title: 'api 1',
            description: 'first api',
            version: '1.0.0',
            protocol: 'mqtt',
            tags: ['test']
          }
        },
        {
          summary: {
            title: 'API 2',
            version: '1.0.0',
            protocol: 'mqtt',
            channelsCount: 3,
            serversCount: 1,
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
          searchableFields: {
            title: 'api 2',
            description: 'second api',
            version: '1.0.0',
            protocol: 'mqtt',
            tags: ['test']
          }
        },
        {
          summary: {
            title: 'API 3',
            version: '2.0.0',
            protocol: 'ws',
            channelsCount: 1,
            serversCount: 2,
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
            createdAt: new Date(),
            updatedAt: new Date(),
            processedAt: new Date()
          },
          searchableFields: {
            title: 'api 3',
            description: 'third api',
            version: '2.0.0',
            protocol: 'ws',
            tags: ['test']
          }
        }
      ];

      for (const doc of testDocs) {
        await mongoService.insertAsyncAPIDocument({
          original: JSON.stringify({ title: doc.summary.title }),
          normalized: doc
        });
      }
    });

    test('should get document statistics', async () => {
      const stats = await mongoService.getDocumentStatistics();

      expect(stats.totalDocuments).toBe(3);
      expect(stats.protocolDistribution).toHaveLength(2);
      expect(stats.versionDistribution).toHaveLength(2);

      const mqttStat = stats.protocolDistribution.find(p => p._id === 'mqtt');
      expect(mqttStat.count).toBe(2);

      const wsStat = stats.protocolDistribution.find(p => p._id === 'ws');
      expect(wsStat.count).toBe(1);

      const v1Stat = stats.versionDistribution.find(v => v._id === '1.0.0');
      expect(v1Stat.count).toBe(2);

      const v2Stat = stats.versionDistribution.find(v => v._id === '2.0.0');
      expect(v2Stat.count).toBe(1);
    });
  });
});
