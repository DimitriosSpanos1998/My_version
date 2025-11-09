const { MongoClient } = require('mongodb');
require('dotenv').config();

class DatabaseConfig {
  constructor() {
    this.client = null;
    this.db = null;
    this.isConnected = false;
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
      this.client = new MongoClient(uri, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      });

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
  getCollection(collectionName = process.env.COLLECTION_NAME || 'asyncapi_specs') {
    const db = this.getDatabase();
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
  async createIndexes(collectionName = process.env.COLLECTION_NAME || 'asyncapi_specs') {
    try {
      const collection = this.getCollection(collectionName);
      
      // Drop existing indexes to avoid conflicts (except _id index)
      try {
        await collection.dropIndexes();
        console.log('🗑️ Dropped existing indexes');
      } catch (error) {
        // Ignore error if no indexes exist or other indexes are present
        console.log('⚠️ Could not drop all indexes (this is usually fine)');
      }
      
      // Create indexes for common query patterns
      await collection.createIndex({ 'metadata.title': 1 });
      await collection.createIndex({ 'metadata.version': 1 });
      await collection.createIndex({ 'metadata.protocol': 1 });
      await collection.createIndex({ 'metadata.createdAt': 1 });
      await collection.createIndex({ 'metadata.updatedAt': 1 });
      // Create a compound text index (MongoDB allows only one text index per collection)
      await collection.createIndex({ 
        'searchableFields.title': 'text',
        'searchableFields.description': 'text'
      });
      await collection.createIndex({ 'searchableFields.tags': 1 });
      
      console.log('📊 Database indexes created successfully');
    } catch (error) {
      console.error('❌ Error creating indexes:', error.message);
      throw error;
    }
  }
}

module.exports = new DatabaseConfig();
