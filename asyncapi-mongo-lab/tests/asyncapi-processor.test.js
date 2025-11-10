process.env.USE_IN_MEMORY_MONGO = 'true';
process.env.DB_NAME = 'asyncapi-processor-test';

const AsyncAPIProcessor = require('../src/processors/asyncapi-processor');
const MongoService = require('../src/services/mongo-service');

describe('AsyncAPI MongoDB Lab Tests', () => {
  let processor;
  let mongoService;

  beforeAll(() => {
    processor = new AsyncAPIProcessor();
    mongoService = new MongoService();
  });

  afterAll(async () => {
    await mongoService.close();
  });

  afterEach(async () => {
    if (mongoService.isDatabaseConnected()) {
      await Promise.all([
        mongoService.getCollection('normalized').deleteMany({}),
        mongoService.getCollection('original').deleteMany({})
      ]);
    }
  });

  describe('AsyncAPIProcessor', () => {
    test('should load and parse AsyncAPI file', async () => {
      const result = await processor.processAsyncAPIFile('src/examples/sample-asyncapi.yaml');
      
      expect(result).toBeDefined();
      expect(result.normalized).toBeDefined();
      expect(result.summary.title).toBe('User Service API');
      expect(result.summary.version).toBe('1.0.0');
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
          // Missing version
        }
      };

      const validation = processor.validateAsyncAPI(invalidSpec);
      expect(validation.isValid).toBe(false);
      expect(validation.errors.length).toBeGreaterThan(0);
    });
  });

  describe('MongoService', () => {
    beforeAll(async () => {
      await mongoService.connect();
    });

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
      expect(foundDoc).not.toBeNull();
      expect(foundDoc.summary.title).toBe(processed.summary.title);

      // Clean up
      await mongoService.deleteAsyncAPIDocument(insertResult.insertedId.toString());
    });

    test('should search documents by text', async () => {
      const results = await mongoService.searchAsyncAPIDocuments('test');
      expect(Array.isArray(results)).toBe(true);
    });

    test('should get document statistics', async () => {
      const stats = await mongoService.getDocumentStatistics();
      expect(stats).toBeDefined();
      expect(stats.totalDocuments).toBeDefined();
      expect(Array.isArray(stats.protocolDistribution)).toBe(true);
    });
  });
});
