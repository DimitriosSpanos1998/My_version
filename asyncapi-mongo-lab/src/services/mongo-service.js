const { ObjectId } = require('mongodb');
const databaseConfig = require('../config/database');

class MongoService {
  /**
   * Connect to MongoDB database
   * @returns {Promise<Object>} Database instance
   */
  async connect() {
    return await databaseConfig.connect();
  }

  /**
   * Get MongoDB collection by logical type
   * @param {string} [collectionType='normalized'] Logical collection type
   * @returns {Object} Collection instance
   */
  getCollection(collectionType = 'normalized') {
    return databaseConfig.getCollection(collectionType);
  }

  getNormalizedCollection() {
    return this.getCollection('normalized');
  }

  getMetadataCollection() {
    return this.getCollection('metadata');
  }

  getOriginalCollection() {
    return this.getCollection('original');
  }

  /**
   * Prepare normalized, metadata, and original content for insertion
   * @param {Object} asyncAPIData - AsyncAPI data payload
   * @returns {Object} Prepared document parts
   */
  prepareDocumentParts(asyncAPIData) {
    if (!asyncAPIData) {
      throw new Error('AsyncAPI data is required');
    }

    const normalizedData = asyncAPIData.normalized
      ? { ...asyncAPIData.normalized }
      : { ...asyncAPIData };

    if (normalizedData._id) {
      delete normalizedData._id;
    }

    const metadataData = asyncAPIData.metadata
      ? { ...asyncAPIData.metadata }
      : normalizedData.metadata
        ? { ...normalizedData.metadata }
        : {};

    if (normalizedData.metadata) {
      delete normalizedData.metadata;
    }

    const originalContent =
      asyncAPIData.original ??
      asyncAPIData.originalContent ??
      null;

    if (originalContent && typeof originalContent === 'object' && originalContent._id) {
      delete originalContent._id;
    }

    const searchableFields = normalizedData.searchableFields
      ? { ...normalizedData.searchableFields }
      : undefined;

    if (searchableFields) {
      normalizedData.searchableFields = searchableFields;
    }

    return {
      normalized: normalizedData,
      metadata: metadataData,
      original: originalContent
    };
  }

  /**
   * Ensure metadata document has required timestamps
   * @param {Object} metadata - Metadata object
   * @returns {Object} Sanitized metadata
   */
  sanitizeMetadata(metadata = {}) {
    const sanitized = { ...metadata };
    const now = new Date();

    const createdAt = metadata.createdAt ? new Date(metadata.createdAt) : new Date(now);
    const updatedAt = metadata.updatedAt ? new Date(metadata.updatedAt) : new Date(createdAt);
    const processedAt = metadata.processedAt ? new Date(metadata.processedAt) : new Date(updatedAt);

    sanitized.createdAt = createdAt;
    sanitized.updatedAt = updatedAt;
    sanitized.processedAt = processedAt;

    return sanitized;
  }

  /**
   * Insert AsyncAPI document across collections
   * @param {Object} asyncAPIData - AsyncAPI processing result or normalized document
   * @returns {Promise<Object>} Insert result with identifiers
   */
  async insertAsyncAPIDocument(asyncAPIData) {
    const { normalized, metadata, original } = this.prepareDocumentParts(asyncAPIData);
    const sanitizedMetadata = this.sanitizeMetadata(metadata);

    const metadataCollection = this.getMetadataCollection();
    const normalizedCollection = this.getNormalizedCollection();
    const originalCollection = this.getOriginalCollection();

    let metadataResult;
    let normalizedResult;
    let originalResult;

    try {
      metadataResult = await metadataCollection.insertOne({ ...sanitizedMetadata });
      const metadataId = metadataResult.insertedId;

      const normalizedDocument = {
        ...normalized,
        metadata: { ...sanitizedMetadata },
        metadataId
      };

      normalizedResult = await normalizedCollection.insertOne(normalizedDocument);
      await metadataCollection.updateOne(
        { _id: metadataId },
        { $set: { normalizedId: normalizedResult.insertedId } }
      );

      const originalDocument = this.buildOriginalDocument(
        original,
        normalizedDocument,
        metadataId,
        normalizedResult.insertedId,
        sanitizedMetadata
      );

      if (originalDocument) {
        originalResult = await originalCollection.insertOne(originalDocument);

        await metadataCollection.updateOne(
          { _id: metadataId },
          { $set: { originalId: originalResult.insertedId } }
        );
      }

      console.log(`💾 Document inserted with ID: ${normalizedResult.insertedId}`);

      return {
        success: true,
        insertedId: normalizedResult.insertedId,
        normalizedId: normalizedResult.insertedId,
        metadataId,
        originalId: originalResult ? originalResult.insertedId : null,
        document: normalizedDocument
      };
    } catch (error) {
      console.error('❌ Error inserting document:', error.message);

      if (originalResult?.insertedId) {
        await originalCollection.deleteOne({ _id: originalResult.insertedId }).catch(() => {});
      }

      if (normalizedResult?.insertedId) {
        await normalizedCollection.deleteOne({ _id: normalizedResult.insertedId }).catch(() => {});
      }

      if (metadataResult?.insertedId) {
        await metadataCollection.deleteOne({ _id: metadataResult.insertedId }).catch(() => {});
      }

      throw error;
    }
  }

  buildOriginalDocument(original, normalizedDocument, metadataId, normalizedId, sanitizedMetadata) {
    let baseDocument = null;

    if (original && typeof original === 'object' && !Array.isArray(original)) {
      baseDocument = { ...original };
    } else if (normalizedDocument) {
      baseDocument = { ...normalizedDocument };
    }

    if (!baseDocument) {
      return null;
    }

    if (baseDocument._id) {
      delete baseDocument._id;
    }

    const metadata = baseDocument.metadata
      ? this.sanitizeMetadata(baseDocument.metadata)
      : { ...sanitizedMetadata };

    return {
      ...baseDocument,
      metadata,
      metadataId,
      normalizedId,
      createdAt: metadata.createdAt,
      updatedAt: metadata.updatedAt
    };
  }

  /**
   * Find AsyncAPI documents with query
   * @param {Object} query - MongoDB query object
   * @param {Object} options - Query options (limit, sort, etc.)
   * @returns {Promise<Array>} Array of matching documents
   */
  async findAsyncAPIDocuments(query = {}, options = {}) {
    try {
      const collection = this.getNormalizedCollection();
      
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
      const collection = this.getNormalizedCollection();
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
      const collection = this.getNormalizedCollection();
      const objectId = typeof id === 'string' ? new ObjectId(id) : id;

      const existingDoc = await collection.findOne(
        { _id: objectId },
        { projection: { metadataId: 1 } }
      );

      if (!existingDoc) {
        throw new Error(`Document with ID ${id} not found`);
      }

      const updateTimestamp = new Date();
      const updateData = {
        ...updates,
        'metadata.updatedAt': updateTimestamp
      };

      const result = await collection.updateOne(
        { _id: objectId },
        { $set: updateData }
      );

      let metadataUpdateResult = { matchedCount: 0, modifiedCount: 0 };
      if (existingDoc.metadataId) {
        const metadataUpdates = this.extractMetadataUpdates(updates);
        metadataUpdates.updatedAt = this.ensureMetadataValue('updatedAt', updateTimestamp);

        metadataUpdateResult = await this.getMetadataCollection().updateOne(
          { _id: existingDoc.metadataId },
          { $set: metadataUpdates }
        );

        await this.getOriginalCollection().updateMany(
          { metadataId: existingDoc.metadataId },
          { $set: { updatedAt: this.ensureMetadataValue('updatedAt', updateTimestamp) } }
        );
      }

      console.log(`✏️ Document updated: ${result.modifiedCount} field(s) modified`);

      return {
        success: true,
        modifiedCount: result.modifiedCount,
        matchedCount: result.matchedCount,
        metadataMatchedCount: metadataUpdateResult.matchedCount,
        metadataModifiedCount: metadataUpdateResult.modifiedCount
      };
    } catch (error) {
      console.error('❌ Error updating document:', error.message);
      throw error;
    }
  }

  /**
   * Extract metadata-specific updates from payload
   * @param {Object} updates - Update payload
   * @returns {Object} Metadata updates
   */
  extractMetadataUpdates(updates = {}) {
    const metadataUpdates = {};

    if (!updates || typeof updates !== 'object') {
      return metadataUpdates;
    }

    if (updates.metadata && typeof updates.metadata === 'object') {
      Object.entries(updates.metadata).forEach(([key, value]) => {
        metadataUpdates[key] = this.ensureMetadataValue(key, value);
      });
    }

    Object.entries(updates).forEach(([key, value]) => {
      if (key.startsWith('metadata.')) {
        const metadataKey = key.replace('metadata.', '');
        metadataUpdates[metadataKey] = this.ensureMetadataValue(metadataKey, value);
      }
    });

    return metadataUpdates;
  }

  /**
   * Normalize metadata field values (e.g. ensure Date instances)
   * @param {string} key - Metadata field key
   * @param {*} value - Field value
   * @returns {*} Normalized value
   */
  ensureMetadataValue(key, value) {
    if (['createdAt', 'updatedAt', 'processedAt'].includes(key) && value) {
      return new Date(value);
    }

    return value;
  }

  /**
   * Delete AsyncAPI document
   * @param {string} id - Document ID
   * @returns {Promise<Object>} Delete result
   */
  async deleteAsyncAPIDocument(id) {
    try {
      const collection = this.getNormalizedCollection();
      const objectId = typeof id === 'string' ? new ObjectId(id) : id;

      const document = await collection.findOne(
        { _id: objectId },
        { projection: { metadataId: 1 } }
      );

      if (!document) {
        console.warn(`⚠️ Document with ID ${id} not found (or already deleted)`);
        return {
          success: true,
          deletedCount: 0,
          metadataDeletedCount: 0,
          originalDeletedCount: 0
        };
      }

      const result = await collection.deleteOne({ _id: objectId });

      let metadataDeletedCount = 0;
      let originalDeletedCount = 0;

      if (document.metadataId) {
        const metadataResult = await this.getMetadataCollection().deleteOne({
          _id: document.metadataId
        });
        metadataDeletedCount = metadataResult.deletedCount;

        const originalResult = await this.getOriginalCollection().deleteMany({
          metadataId: document.metadataId
        });
        originalDeletedCount = originalResult.deletedCount;
      }

      console.log(`🗑️ Document deleted: ${result.deletedCount} document(s) removed`);

      return {
        success: true,
        deletedCount: result.deletedCount,
        metadataDeletedCount,
        originalDeletedCount
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
      const collection = this.getNormalizedCollection();
      
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
      const collection = this.getNormalizedCollection();
      
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
