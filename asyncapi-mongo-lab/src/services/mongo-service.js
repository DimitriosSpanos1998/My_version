const { ObjectId } = require('mongodb');
const yaml = require('yaml');
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

  getOriginalCollection() {
    return this.getCollection('original');
  }

  /**
   * Prepare normalized, summary, and original content for insertion
   * @param {Object} asyncAPIData - AsyncAPI data payload
   * @returns {Object} Prepared document parts
   */
  prepareDocumentParts(asyncAPIData) {
    if (!asyncAPIData) {
      throw new Error('AsyncAPI data is required');
    }

    const normalizedData = asyncAPIData.normalized
      ? JSON.parse(JSON.stringify(asyncAPIData.normalized))
      : JSON.parse(JSON.stringify(asyncAPIData));

    if (normalizedData._id) {
      delete normalizedData._id;
    }

    const summary = this.ensureSummary(
      asyncAPIData.summary ?? normalizedData.summary ?? {},
      normalizedData
    );

    const searchableFields = asyncAPIData.searchableFields
      ? { ...asyncAPIData.searchableFields }
      : normalizedData.searchableFields
        ? { ...normalizedData.searchableFields }
        : this.buildSearchableFieldsFromSummary(summary);

    normalizedData.summary = summary;
    normalizedData.searchableFields = searchableFields;

    const originalContent = this.prepareOriginalContent(
      asyncAPIData.original ??
      asyncAPIData.originalContent ??
      asyncAPIData.raw ??
      null
    );

    return {
      normalized: normalizedData,
      summary,
      searchableFields,
      original: originalContent
    };
  }

  ensureSummary(summary = {}, normalizedData = {}) {
    const cloned = { ...summary };
    const now = new Date();

    if (!cloned.title && normalizedData?.info?.title) {
      cloned.title = normalizedData.info.title;
    }

    if (!cloned.version && normalizedData?.info?.version) {
      cloned.version = normalizedData.info.version;
    }

    if (!cloned.description && normalizedData?.info?.description) {
      cloned.description = normalizedData.info.description;
    }

    if (!cloned.protocol) {
      const firstProtocol = Object.values(normalizedData?.servers ?? {})
        .map(server => server?.protocol)
        .find(Boolean);
      cloned.protocol = firstProtocol || 'unknown';
    }

    const channelsCount = Array.isArray(normalizedData?.channels)
      ? normalizedData.channels.length
      : normalizedData?.channels
        ? Object.keys(normalizedData.channels).length
        : 0;
    if (cloned.channelsCount == null) {
      cloned.channelsCount = channelsCount;
    }

    const serversCount = Array.isArray(normalizedData?.servers)
      ? normalizedData.servers.length
      : normalizedData?.servers
        ? Object.keys(normalizedData.servers).length
        : 0;
    if (cloned.serversCount == null) {
      cloned.serversCount = serversCount;
    }

    if (!cloned.defaultContentType && normalizedData?.defaultContentType) {
      cloned.defaultContentType = normalizedData.defaultContentType;
    }

    const createdAt = cloned.createdAt ? new Date(cloned.createdAt) : now;
    const updatedAt = cloned.updatedAt ? new Date(cloned.updatedAt) : createdAt;

    cloned.createdAt = createdAt;
    cloned.updatedAt = updatedAt;

    return cloned;
  }

  buildSearchableFieldsFromSummary(summary = {}) {
    const toLower = value => (typeof value === 'string' ? value.toLowerCase() : '');

    const tags = Array.isArray(summary.tags)
      ? summary.tags
      : summary.tags
        ? [summary.tags]
        : [];

    return {
      title: toLower(summary.title || ''),
      description: toLower(summary.description || ''),
      version: summary.version || '',
      protocol: toLower(summary.protocol || ''),
      tags: Array.from(new Set(tags.filter(Boolean).map(tag => toLower(tag))))
    };
  }

  /**
   * Insert AsyncAPI document across collections
   * @param {Object} asyncAPIData - AsyncAPI processing result or normalized document
   * @returns {Promise<Object>} Insert result with identifiers
   */
  async insertAsyncAPIDocument(asyncAPIData) {
    const { normalized, summary, searchableFields, original } = this.prepareDocumentParts(asyncAPIData);
    const normalizedCollection = this.getNormalizedCollection();
    const originalCollection = this.getOriginalCollection();

    let normalizedResult;
    let originalResult;

    try {
      const normalizedDocument = {
        ...normalized,
        summary,
        searchableFields
      };

      normalizedResult = await normalizedCollection.insertOne(normalizedDocument);

      const originalDocument = this.buildOriginalDocument(
        original,
        normalizedDocument,
        normalizedResult.insertedId
      );

      if (originalDocument) {
        originalResult = await originalCollection.insertOne(originalDocument);
      }

      console.log(`💾 Document inserted with ID: ${normalizedResult.insertedId}`);

      return {
        success: true,
        insertedId: normalizedResult.insertedId,
        normalizedId: normalizedResult.insertedId,
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

      throw error;
    }
  }

  prepareOriginalContent(originalContent) {
    if (!originalContent) {
      return null;
    }

    if (originalContent && typeof originalContent === 'object' && originalContent._id) {
      const cloned = JSON.parse(JSON.stringify(originalContent));
      delete cloned._id;
      return cloned;
    }

    return originalContent;
  }

  parseOriginalContentToObject(originalContent) {
    if (typeof originalContent !== 'string') {
      return null;
    }

    const trimmed = originalContent.trim();
    if (!trimmed) {
      return null;
    }

    try {
      if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
        return JSON.parse(trimmed);
      }
    } catch (error) {
      // Fallback to YAML parsing below
    }

    try {
      return yaml.parse(trimmed);
    } catch (error) {
      return null;
    }
  }

  buildOriginalDocument(original, normalizedDocument, normalizedId) {
    let documentContent = null;
    let rawContent = null;

    if (original && typeof original === 'object' && !Array.isArray(original)) {
      const cloned = JSON.parse(JSON.stringify(original));
      if (cloned.raw || cloned.rawContent) {
        rawContent = cloned.raw ?? cloned.rawContent;
        delete cloned.raw;
        delete cloned.rawContent;
      }
      documentContent = cloned;
    } else if (typeof original === 'string') {
      rawContent = original;
      documentContent = this.parseOriginalContentToObject(original);
    }

    if (!documentContent && normalizedDocument) {
      documentContent = JSON.parse(JSON.stringify(normalizedDocument));
    }

    if (!documentContent) {
      return null;
    }

    if (documentContent._id) {
      delete documentContent._id;
    }

    const baseOriginalDocument = {
      normalizedId,
      createdAt: new Date(),
      updatedAt: new Date(),
      document: documentContent
    };

    if (rawContent) {
      baseOriginalDocument.rawContent = rawContent;
    }

    return baseOriginalDocument;
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
        sort = { 'summary.createdAt': -1 },
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

      const updateTimestamp = new Date();
      const setUpdates = {};
      let affectsSummary = false;

      Object.entries(updates || {}).forEach(([key, value]) => {
        if (key === 'summary' && value && typeof value === 'object') {
          affectsSummary = true;
          Object.entries(value).forEach(([innerKey, innerValue]) => {
            setUpdates[`summary.${innerKey}`] = innerValue;
          });
        } else {
          setUpdates[key] = value;
          if (key.startsWith('summary.')) {
            affectsSummary = true;
          }
        }
      });

      setUpdates['summary.updatedAt'] = updateTimestamp;

      const result = await collection.updateOne(
        { _id: objectId },
        { $set: setUpdates }
      );

      if (result.matchedCount === 0) {
        throw new Error(`Document with ID ${id} not found`);
      }

      if (result.modifiedCount > 0) {
        const doc = await collection.findOne(
          { _id: objectId },
          { projection: { summary: 1 } }
        );

        if (doc?.summary) {
          const refreshedSearchable = this.buildSearchableFieldsFromSummary(doc.summary);
          await collection.updateOne(
            { _id: objectId },
            { $set: { searchableFields: refreshedSearchable } }
          );
        }

        await this.getOriginalCollection().updateMany(
          { normalizedId: objectId },
          { $set: { updatedAt: updateTimestamp } }
        );
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
      const collection = this.getNormalizedCollection();
      const objectId = typeof id === 'string' ? new ObjectId(id) : id;

      const document = await collection.findOne({ _id: objectId });

      if (!document) {
        console.warn(`⚠️ Document with ID ${id} not found (or already deleted)`);
        return {
          success: true,
          deletedCount: 0,
          originalDeletedCount: 0
        };
      }

      const result = await collection.deleteOne({ _id: objectId });

      let originalDeletedCount = 0;

      const originalResult = await this.getOriginalCollection().deleteMany({
        normalizedId: objectId
      });
      originalDeletedCount = originalResult.deletedCount;

      console.log(`🗑️ Document deleted: ${result.deletedCount} document(s) removed`);

      return {
        success: true,
        deletedCount: result.deletedCount,
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
