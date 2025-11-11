const AsyncAPIProcessor = require('../src/processors/asyncapi-processor');
const MongoService = require('../src/services/mongo-service');
const { createMockDatabaseConfig } = require('./helpers/mock-database-config');

describe('AsyncAPI MongoDB Lab Tests', () => {
  let processor;
  let mongoService;
  let mockConfig;

  const clearCollections = async () => {
    if (!mongoService || !mongoService.isDatabaseConnected()) {
      return;
    }

    await Promise.all([
      mongoService.getCollection('normalized').deleteMany({}),
      mongoService.getCollection('original').deleteMany({}),
      mongoService.getCollection('metada').deleteMany({})
    ]);
  };

  beforeAll(async () => {
    mockConfig = createMockDatabaseConfig();
    processor = new AsyncAPIProcessor({ db: mockConfig });
    mongoService = new MongoService(mockConfig);
    await mongoService.connect();
  });

  afterAll(async () => {
    await clearCollections();
    await mongoService.close();
  });

  afterEach(async () => {
    await clearCollections();
  });

  describe('AsyncAPIProcessor', () => {
    test('should load and parse AsyncAPI file', async () => {
      const result = await processor.processAsyncAPIFile('src/examples/sample-asyncapi.yaml');

      expect(result).toBeDefined();
      expect(result.normalized).toBeDefined();
      expect(result.summary.title).toBe('User Service API');
      expect(result.summary.version).toBe('1.0.0');
      expect(Array.isArray(result.asyncService?.AsyncService)).toBe(true);
      expect(result.asyncService.AsyncService[0].title).toBe('User Service API');
    });

    test('should validate AsyncAPI specification', () => {
      const validSpec = {
        info: {
          title: 'Test API',
          version: '1.0.0'
        },
        channels: {}
      };

      const validation = processor.validateAsyncAPI(validSpec);
      expect(validation.isValid).toBe(true);
    });

    test('should detect invalid AsyncAPI specification', () => {
      const invalidSpec = {
        info: {
          title: 'Test API'
        }
      };

      const validation = processor.validateAsyncAPI(invalidSpec);
      expect(validation.isValid).toBe(false);
      expect(validation.errors.length).toBeGreaterThan(0);
    });
  });

  describe('MongoService', () => {
    test('should connect to MongoDB', () => {
      expect(mongoService.isDatabaseConnected()).toBe(true);
    });

    test('should insert and find document', async () => {
      const processed = await processor.processAsyncAPIFile('src/examples/sample-asyncapi.yaml');

      const insertResult = await mongoService.insertAsyncAPIDocument(processed);
      expect(insertResult.success).toBe(true);
      expect(insertResult.insertedId).toBeDefined();

      const foundDoc = await mongoService.findAsyncAPIDocumentById(insertResult.insertedId.toString());
      expect(foundDoc).toBeDefined();
      expect(foundDoc.summary.title).toBe(processed.summary.title);

      await mongoService.deleteAsyncAPIDocument(insertResult.insertedId.toString());
    });

    test('should search documents by text', async () => {
      const processed = await processor.processAsyncAPIFile('src/examples/sample-asyncapi.yaml');
      await mongoService.insertAsyncAPIDocument(processed);

      const results = await mongoService.searchAsyncAPIDocuments('user');
      expect(Array.isArray(results)).toBe(true);
      expect(results.length).toBeGreaterThan(0);
    });

    test('should get document statistics', async () => {
      const processed = await processor.processAsyncAPIFile('src/examples/sample-asyncapi.yaml');
      await mongoService.insertAsyncAPIDocument(processed);

      const stats = await mongoService.getDocumentStatistics();
      expect(stats).toBeDefined();
      expect(stats.totalDocuments).toBeGreaterThan(0);
      expect(Array.isArray(stats.protocolDistribution)).toBe(true);
    });
  });
});
