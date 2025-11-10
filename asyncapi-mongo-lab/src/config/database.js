const { MongoClient } = require('mongodb');
require('dotenv').config();

class DatabaseConfig {
  constructor() {
    this.client = null;
    this.db = null;
    this.isConnected = false;
  }

  /**
   * Resolve configured collection names
   * @returns {Object} Collection names keyed by logical type
   */
  getCollectionNames() {
    return {
      original: process.env.ORIGINAL_COLLECTION_NAME || 'asyncapi_originals',
      normalized:
        process.env.NORMALIZED_COLLECTION_NAME ||
        process.env.COLLECTION_NAME ||
        'asyncapi_normalized'
    };
  }

  /**
   * Resolve collection name from logical type or explicit name
   * @param {string} [collectionTypeOrName='normalized'] Logical type or explicit collection name
   * @returns {string} MongoDB collection name
   */
  getCollectionName(collectionTypeOrName = 'normalized') {
    const names = this.getCollectionNames();

    if (!collectionTypeOrName) {
      return names.normalized;
    }

    return names[collectionTypeOrName] || collectionTypeOrName;
  }

  /**
   * Connect to MongoDB database
   * @returns {Promise<Object>} Database instance
   */
  async connect() {
    try {
      if (this.isConnected && this.db) {
        return this.db;
      }

      const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017/asyncapi-lab';
      const dbName = process.env.DB_NAME || 'asyncapi-lab';

      console.log('🔌 Connecting to MongoDB...');
      this.client = new MongoClient(uri);

      await this.client.connect();
      this.db = this.client.db(dbName);
      this.isConnected = true;

      console.log(`✅ Connected to MongoDB database: ${dbName}`);
      return this.db;
    } catch (error) {
      console.error('❌ MongoDB connection error:', error.message);
      throw error;
    }
  }

  /**
   * Get database instance
   * @returns {Object} Database instance
   */
  getDatabase() {
    if (!this.isConnected || !this.db) {
      throw new Error('Database not connected. Call connect() first.');
    }
    return this.db;
  }

  /**
   * Get collection instance
   * @param {string} collectionName - Name of the collection
   * @returns {Object} Collection instance
   */
  getCollection(collectionTypeOrName = 'normalized') {
    const db = this.getDatabase();
    const collectionName = this.getCollectionName(collectionTypeOrName);
    return db.collection(collectionName);
  }

  /**
   * Close database connection
   */
  async close() {
    if (this.client) {
      await this.client.close();
      this.isConnected = false;
      this.db = null;
      this.client = null;
      console.log('🔌 MongoDB connection closed');
    }
  }

  /**
   * Check if database is connected
   * @returns {boolean} Connection status
   */
  isDatabaseConnected() {
    return this.isConnected;
  }

  /**
   * Create indexes for better performance
   * @param {string} collectionName - Name of the collection
   */
  async createIndexes() {
    try {
      await this.createNormalizedIndexes();
      await this.createOriginalIndexes();
      console.log('📊 Database indexes created successfully');
    } catch (error) {
      console.error('❌ Error creating indexes:', error.message);
      throw error;
    }
  }

  /**
   * Create indexes for normalized documents collection
   */
  async createNormalizedIndexes() {
    const collection = this.getCollection('normalized');

    await this.dropCollectionIndexes(collection, 'normalized');

    await collection.createIndex({ 'summary.title': 1 }, { name: 'summary_title_idx' });
    await collection.createIndex({ 'summary.version': 1 }, { name: 'summary_version_idx' });
    await collection.createIndex({ 'summary.protocol': 1 }, { name: 'summary_protocol_idx' });
    await collection.createIndex({ 'summary.createdAt': 1 }, { name: 'summary_created_at_idx' });
    await collection.createIndex({ 'summary.updatedAt': 1 }, { name: 'summary_updated_at_idx' });
    await collection.createIndex({ 'searchableFields.tags': 1 }, { name: 'searchable_tags_idx' });
    await collection.createIndex(
      {
        'searchableFields.title': 'text',
        'searchableFields.description': 'text'
      },
      { name: 'searchable_text_idx' }
    );
  }

  /**
   * Create indexes for original documents collection
   */
  async createOriginalIndexes() {
    const collection = this.getCollection('original');

    await this.dropCollectionIndexes(collection, 'original');

    await collection.createIndex({ normalizedId: 1 }, { name: 'normalized_id_idx' });
    await collection.createIndex({ createdAt: 1 }, { name: 'created_at_idx' });
  }

  /**
   * Drop existing indexes for a collection (ignoring _id)
   * @param {Object} collection - MongoDB collection
   * @param {string} label - Collection label for logging
   */
  async dropCollectionIndexes(collection, label) {
    try {
      await collection.dropIndexes();
      console.log(`🗑️ Dropped existing indexes for ${label} collection`);
    } catch (error) {
      // Ignore errors when indexes do not exist yet
      console.log(`ℹ️ No existing indexes to drop for ${label} collection`);
    }
  }
}

module.exports = new DatabaseConfig();
