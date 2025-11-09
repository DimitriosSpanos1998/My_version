const databaseConfig = require('../config/database');

class MongoService {
  constructor() {
    this.collectionName = process.env.COLLECTION_NAME || 'asyncapi_specs';
  }

  /**
   * Connect to MongoDB database
   * @returns {Promise<Object>} Database instance
   */
  async connect() {
    return await databaseConfig.connect();
  }

  /**
   * Get collection instance
   * @returns {Object} Collection instance
   */
  getCollection() {
    return databaseConfig.getCollection(this.collectionName);
  }

  /**
   * Insert AsyncAPI document into MongoDB
   * @param {Object} asyncAPIData - Normalized AsyncAPI data
   * @returns {Promise<Object>} Insert result with document ID
   */
  async insertAsyncAPIDocument(asyncAPIData) {
    try {
      const collection = this.getCollection();
      
      // Add timestamp
      const document = {
        ...asyncAPIData,
        metadata: {
          ...asyncAPIData.metadata,
          createdAt: new Date(),
          updatedAt: new Date()
        }
      };

      const result = await collection.insertOne(document);
      console.log(`💾 Document inserted with ID: ${result.insertedId}`);
      
      return {
        success: true,
        insertedId: result.insertedId,
        document: document
      };
    } catch (error) {
      console.error('❌ Error inserting document:', error.message);
      throw error;
    }
  }

  /**
   * Find AsyncAPI documents with query
   * @param {Object} query - MongoDB query object
   * @param {Object} options - Query options (limit, sort, etc.)
   * @returns {Promise<Array>} Array of matching documents
   */
  async findAsyncAPIDocuments(query = {}, options = {}) {
    try {
      const collection = this.getCollection();
      
      const {
        limit = 10,
        sort = { 'metadata.createdAt': -1 },
        projection = null
      } = options;

      let cursor = collection.find(query);
      
      if (sort) cursor = cursor.sort(sort);
      if (limit) cursor = cursor.limit(limit);
      if (projection) cursor = cursor.project(projection);

      const documents = await cursor.toArray();
      console.log(`🔍 Found ${documents.length} documents`);
      
      return documents;
    } catch (error) {
      console.error('❌ Error finding documents:', error.message);
      throw error;
    }
  }

  /**
   * Find AsyncAPI document by ID
   * @param {string} id - Document ID
   * @returns {Promise<Object|null>} Document or null if not found
   */
  async findAsyncAPIDocumentById(id) {
    try {
      const collection = this.getCollection();
      const { ObjectId } = require('mongodb');
      
      const document = await collection.findOne({ _id: new ObjectId(id) });
      
      if (document) {
        console.log(`🔍 Found document with ID: ${id}`);
      } else {
        console.log(`❌ Document not found with ID: ${id}`);
      }
      
      return document;
    } catch (error) {
      console.error('❌ Error finding document by ID:', error.message);
      throw error;
    }
  }

  /**
   * Update AsyncAPI document
   * @param {string} id - Document ID
   * @param {Object} updates - Fields to update
   * @returns {Promise<Object>} Update result
   */
  async updateAsyncAPIDocument(id, updates) {
    try {
      const collection = this.getCollection();
      const { ObjectId } = require('mongodb');
      
      // Add update timestamp
      const updateData = {
        ...updates,
        'metadata.updatedAt': new Date()
      };

      const result = await collection.updateOne(
        { _id: new ObjectId(id) },
        { $set: updateData }
      );

      if (result.matchedCount === 0) {
        throw new Error(`Document with ID ${id} not found`);
      }

      console.log(`✏️ Document updated: ${result.modifiedCount} field(s) modified`);
      
      return {
        success: true,
        modifiedCount: result.modifiedCount,
        matchedCount: result.matchedCount
      };
    } catch (error) {
      console.error('❌ Error updating document:', error.message);
      throw error;
    }
  }

  /**
   * Delete AsyncAPI document
   * @param {string} id - Document ID
   * @returns {Promise<Object>} Delete result
   */
  async deleteAsyncAPIDocument(id) {
    try {
      const collection = this.getCollection();
      const { ObjectId } = require('mongodb');
      
      // Handle both string and ObjectId
      const objectId = typeof id === 'string' ? new ObjectId(id) : id;
      
      const result = await collection.deleteOne({ _id: objectId });

      if (result.deletedCount === 0) {
        console.warn(`⚠️ Document with ID ${id} not found (or already deleted)`);
        // Return success even if not found to avoid throwing in test cleanup
        return {
          success: true,
          deletedCount: 0
        };
      }

      console.log(`🗑️ Document deleted: ${result.deletedCount} document(s) removed`);
      
      return {
        success: true,
        deletedCount: result.deletedCount
      };
    } catch (error) {
      console.error('❌ Error deleting document:', error.message);
      throw error;
    }
  }

  /**
   * Get all AsyncAPI documents
   * @param {Object} options - Query options
   * @returns {Promise<Array>} All documents
   */
  async getAllAsyncAPIDocuments(options = {}) {
    return await this.findAsyncAPIDocuments({}, options);
  }

  /**
   * Search AsyncAPI documents by text
   * @param {string} searchText - Text to search for
   * @param {Object} options - Search options
   * @returns {Promise<Array>} Matching documents
   */
  async searchAsyncAPIDocuments(searchText, options = {}) {
    try {
      const collection = this.getCollection();
      
      const searchQuery = {
        $or: [
          { 'searchableFields.title': { $regex: searchText, $options: 'i' } },
          { 'searchableFields.description': { $regex: searchText, $options: 'i' } },
          { 'searchableFields.tags': { $regex: searchText, $options: 'i' } }
        ]
      };

      return await this.findAsyncAPIDocuments(searchQuery, options);
    } catch (error) {
      console.error('❌ Error searching documents:', error.message);
      throw error;
    }
  }

  /**
   * Find documents by protocol
   * @param {string} protocol - Protocol name (mqtt, websocket, etc.)
   * @param {Object} options - Query options
   * @returns {Promise<Array>} Matching documents
   */
  async findDocumentsByProtocol(protocol, options = {}) {
    const query = { 'searchableFields.protocol': protocol.toLowerCase() };
    return await this.findAsyncAPIDocuments(query, options);
  }

  /**
   * Find documents by version
   * @param {string} version - Version string
   * @param {Object} options - Query options
   * @returns {Promise<Array>} Matching documents
   */
  async findDocumentsByVersion(version, options = {}) {
    const query = { 'searchableFields.version': version };
    return await this.findAsyncAPIDocuments(query, options);
  }

  /**
   * Get document statistics
   * @returns {Promise<Object>} Statistics about the collection
   */
  async getDocumentStatistics() {
    try {
      const collection = this.getCollection();
      
      const totalCount = await collection.countDocuments();
      const protocolStats = await collection.aggregate([
        { $group: { _id: '$searchableFields.protocol', count: { $sum: 1 } } },
        { $sort: { count: -1 } }
      ]).toArray();
      
      const versionStats = await collection.aggregate([
        { $group: { _id: '$searchableFields.version', count: { $sum: 1 } } },
        { $sort: { count: -1 } }
      ]).toArray();

      const stats = {
        totalDocuments: totalCount,
        protocolDistribution: protocolStats,
        versionDistribution: versionStats,
        lastUpdated: new Date()
      };

      console.log('📊 Document statistics retrieved');
      return stats;
    } catch (error) {
      console.error('❌ Error getting statistics:', error.message);
      throw error;
    }
  }

  /**
   * Check if database is connected
   * @returns {boolean} Connection status
   */
  isDatabaseConnected() {
    return databaseConfig.isDatabaseConnected();
  }

  /**
   * Close database connection
   */
  async close() {
    await databaseConfig.close();
  }
}

module.exports = MongoService;
