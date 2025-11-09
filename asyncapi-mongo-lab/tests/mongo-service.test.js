process.env.USE_IN_MEMORY_MONGO = 'true';
process.env.DB_NAME = 'mongo-service-test';

const MongoService = require('../src/services/mongo-service');

describe('MongoService Integration Tests', () => {
  let mongoService;

  const clearCollections = async () => {
    if (!mongoService || !mongoService.isDatabaseConnected()) {
      return;
    }

    const normalized = mongoService.getCollection('normalized');
    const metadata = mongoService.getCollection('metadata');
    const original = mongoService.getCollection('original');

    await Promise.all([
      normalized.deleteMany({}),
      metadata.deleteMany({}),
      original.deleteMany({})
    ]);
  };

  beforeAll(async () => {
    mongoService = new MongoService();
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
        metadata: {
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

      // Create
      const insertResult = await mongoService.insertAsyncAPIDocument({
        original: JSON.stringify({ title: testDoc.metadata.title }),
        normalized: testDoc
      });
      expect(insertResult.success).toBe(true);
      const docId = insertResult.insertedId.toString();
      expect(insertResult.metadataId).toBeDefined();
      expect(insertResult.originalId).toBeDefined();

      const metadataCollection = mongoService.getCollection('metadata');
      const originalCollection = mongoService.getCollection('original');

      const storedMetadata = await metadataCollection.findOne({ _id: insertResult.metadataId });
      expect(storedMetadata).toBeDefined();
      expect(storedMetadata.title).toBe('CRUD Test API');

      const storedOriginal = await originalCollection.findOne({ metadataId: insertResult.metadataId });
      expect(storedOriginal).toBeDefined();
      expect(storedOriginal.normalizedId.toString()).toBe(docId);

      // Read
      const foundDoc = await mongoService.findAsyncAPIDocumentById(docId);
      expect(foundDoc).toBeDefined();
      expect(foundDoc.metadata.title).toBe('CRUD Test API');

      // Update
      const updateResult = await mongoService.updateAsyncAPIDocument(docId, {
        'metadata.description': 'Updated description',
        'metadata.version': '1.1.0'
      });
      expect(updateResult.success).toBe(true);
      expect(updateResult.modifiedCount).toBe(1);
      expect(updateResult.metadataMatchedCount).toBe(1);

      // Verify update
      const updatedDoc = await mongoService.findAsyncAPIDocumentById(docId);
      expect(updatedDoc.metadata.description).toBe('Updated description');
      expect(updatedDoc.metadata.version).toBe('1.1.0');

      const updatedMetadata = await metadataCollection.findOne({ _id: insertResult.metadataId });
      expect(updatedMetadata.description).toBe('Updated description');
      expect(updatedMetadata.version).toBe('1.1.0');

      // Delete
      const deleteResult = await mongoService.deleteAsyncAPIDocument(docId);
      expect(deleteResult.success).toBe(true);
      expect(deleteResult.deletedCount).toBe(1);
      expect(deleteResult.metadataDeletedCount).toBe(1);

      // Verify deletion
      const deletedDoc = await mongoService.findAsyncAPIDocumentById(docId);
      expect(deletedDoc).toBeNull();

      const metadataAfterDelete = await metadataCollection.findOne({ _id: insertResult.metadataId });
      expect(metadataAfterDelete).toBeNull();

      const remainingOriginal = await originalCollection.countDocuments({ metadataId: insertResult.metadataId });
      expect(remainingOriginal).toBe(0);
    });
  });

  describe('Query Operations', () => {
    beforeEach(async () => {
      // Insert test data
      const testDocs = [
        {
          metadata: {
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
          metadata: {
            title: 'WebSocket API',
            version: '2.0.0',
            protocol: 'ws',
            channelsCount: 3,
            serversCount: 2,
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
          original: JSON.stringify({ title: doc.metadata.title }),
          normalized: doc
        });
      }
    });

    test('should find documents by protocol', async () => {
      const mqttDocs = await mongoService.findDocumentsByProtocol('mqtt');
      expect(mqttDocs).toHaveLength(1);
      expect(mqttDocs[0].metadata.protocol).toBe('mqtt');

      const wsDocs = await mongoService.findDocumentsByProtocol('ws');
      expect(wsDocs).toHaveLength(1);
      expect(wsDocs[0].metadata.protocol).toBe('ws');
    });

    test('should find documents by version', async () => {
      const v1Docs = await mongoService.findDocumentsByVersion('1.0.0');
      expect(v1Docs).toHaveLength(1);
      expect(v1Docs[0].metadata.version).toBe('1.0.0');

      const v2Docs = await mongoService.findDocumentsByVersion('2.0.0');
      expect(v2Docs).toHaveLength(1);
      expect(v2Docs[0].metadata.version).toBe('2.0.0');
    });

    test('should search documents by text', async () => {
      const mqttResults = await mongoService.searchAsyncAPIDocuments('mqtt');
      expect(mqttResults).toHaveLength(1);
      expect(mqttResults[0].metadata.title).toBe('MQTT API');

      const wsResults = await mongoService.searchAsyncAPIDocuments('websocket');
      expect(wsResults).toHaveLength(1);
      expect(wsResults[0].metadata.title).toBe('WebSocket API');
    });

    test('should find documents with complex queries', async () => {
      const multiChannelDocs = await mongoService.findAsyncAPIDocuments({
        'metadata.channelsCount': { $gt: 3 }
      });
      expect(multiChannelDocs).toHaveLength(1);
      expect(multiChannelDocs[0].metadata.title).toBe('MQTT API');

      const multiServerDocs = await mongoService.findAsyncAPIDocuments({
        'metadata.serversCount': { $gte: 2 }
      });
      expect(multiServerDocs).toHaveLength(1);
      expect(multiServerDocs[0].metadata.title).toBe('WebSocket API');
    });
  });

  describe('Statistics and Aggregations', () => {
    beforeEach(async () => {
      // Insert diverse test data
      const testDocs = [
        {
          metadata: {
            title: 'API 1',
            version: '1.0.0',
            protocol: 'mqtt',
            channelsCount: 2,
            serversCount: 1,
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
          metadata: {
            title: 'API 2',
            version: '1.0.0',
            protocol: 'mqtt',
            channelsCount: 3,
            serversCount: 1,
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
          metadata: {
            title: 'API 3',
            version: '2.0.0',
            protocol: 'ws',
            channelsCount: 1,
            serversCount: 2,
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
          original: JSON.stringify({ title: doc.metadata.title }),
          normalized: doc
        });
      }
    });

    test('should get document statistics', async () => {
      const stats = await mongoService.getDocumentStatistics();
      
      expect(stats.totalDocuments).toBe(3);
      expect(stats.protocolDistribution).toHaveLength(2);
      expect(stats.versionDistribution).toHaveLength(2);

      // Check protocol distribution
      const mqttStat = stats.protocolDistribution.find(p => p._id === 'mqtt');
      expect(mqttStat.count).toBe(2);

      const wsStat = stats.protocolDistribution.find(p => p._id === 'ws');
      expect(wsStat.count).toBe(1);

      // Check version distribution
      const v1Stat = stats.versionDistribution.find(v => v._id === '1.0.0');
      expect(v1Stat.count).toBe(2);

      const v2Stat = stats.versionDistribution.find(v => v._id === '2.0.0');
      expect(v2Stat.count).toBe(1);
    });
  });
});
