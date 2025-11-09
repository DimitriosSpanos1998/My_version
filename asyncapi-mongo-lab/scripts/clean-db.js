#!/usr/bin/env node

require('dotenv').config();
const MongoService = require('../src/services/mongo-service');

class DatabaseCleaner {
  constructor() {
    this.mongoService = new MongoService();
  }

  /**
   * Clean database (remove all documents)
   */
  async clean() {
    try {
      console.log('🧹 Cleaning MongoDB database...\n');

      await this.mongoService.connect();
      const normalizedCollection = this.mongoService.getCollection('normalized');
      const metadataCollection = this.mongoService.getCollection('metadata');
      const originalCollection = this.mongoService.getCollection('original');

      const [normalizedBefore, metadataBefore, originalBefore] = await Promise.all([
        normalizedCollection.countDocuments(),
        metadataCollection.countDocuments(),
        originalCollection.countDocuments()
      ]);

      console.log(`📊 Normalized before cleanup: ${normalizedBefore}`);
      console.log(`📊 Metadata before cleanup: ${metadataBefore}`);
      console.log(`📊 Original before cleanup: ${originalBefore}`);

      if (normalizedBefore === 0 && metadataBefore === 0 && originalBefore === 0) {
        console.log('✅ Database is already clean!');
        return;
      }

      const [normalizedResult, metadataResult, originalResult] = await Promise.all([
        normalizedCollection.deleteMany({}),
        metadataCollection.deleteMany({}),
        originalCollection.deleteMany({})
      ]);

      console.log(`🗑️ Deleted ${normalizedResult.deletedCount} normalized documents`);
      console.log(`🗑️ Deleted ${metadataResult.deletedCount} metadata documents`);
      console.log(`🗑️ Deleted ${originalResult.deletedCount} original documents`);

      const [normalizedAfter, metadataAfter, originalAfter] = await Promise.all([
        normalizedCollection.countDocuments(),
        metadataCollection.countDocuments(),
        originalCollection.countDocuments()
      ]);

      console.log(`📊 Normalized after cleanup: ${normalizedAfter}`);
      console.log(`📊 Metadata after cleanup: ${metadataAfter}`);
      console.log(`📊 Original after cleanup: ${originalAfter}`);

      if (normalizedAfter === 0 && metadataAfter === 0 && originalAfter === 0) {
        console.log('✅ Database cleaned successfully!');
      } else {
        console.log('⚠️ Some documents may still remain');
      }

    } catch (error) {
      console.error('❌ Database cleanup failed:', error.message);
      process.exit(1);
    } finally {
      await this.mongoService.close();
    }
  }

  /**
   * Clean old documents (older than specified days)
   */
  async cleanOld(days = 7) {
    try {
      console.log(`🧹 Cleaning documents older than ${days} days...\n`);

      await this.mongoService.connect();
      const normalizedCollection = this.mongoService.getCollection('normalized');
      const metadataCollection = this.mongoService.getCollection('metadata');
      const originalCollection = this.mongoService.getCollection('original');

      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - days);

      const normalizedFilter = { 'metadata.createdAt': { $lt: cutoffDate } };

      const normalizedBefore = await normalizedCollection.countDocuments(normalizedFilter);
      console.log(`📊 Normalized docs older than ${days} days: ${normalizedBefore}`);

      if (normalizedBefore === 0) {
        console.log('✅ No old documents to clean!');
        return;
      }

      const docsToDelete = await normalizedCollection
        .find(normalizedFilter, { projection: { metadataId: 1 } })
        .toArray();

      const metadataIds = docsToDelete
        .map(doc => doc.metadataId)
        .filter(Boolean);

      const [normalizedResult, metadataResult, originalResult] = await Promise.all([
        normalizedCollection.deleteMany({ _id: { $in: docsToDelete.map(doc => doc._id) } }),
        metadataIds.length
          ? metadataCollection.deleteMany({ _id: { $in: metadataIds } })
          : Promise.resolve({ deletedCount: 0 }),
        metadataIds.length
          ? originalCollection.deleteMany({ metadataId: { $in: metadataIds } })
          : Promise.resolve({ deletedCount: 0 })
      ]);

      console.log(`🗑️ Deleted ${normalizedResult.deletedCount} normalized documents`);
      console.log(`🗑️ Deleted ${metadataResult.deletedCount || 0} metadata documents`);
      console.log(`🗑️ Deleted ${originalResult.deletedCount || 0} original documents`);

      console.log('✅ Old documents cleaned successfully!');

    } catch (error) {
      console.error('❌ Old documents cleanup failed:', error.message);
      process.exit(1);
    } finally {
      await this.mongoService.close();
    }
  }
}

// Run cleanup if this file is executed directly
if (require.main === module) {
  const cleaner = new DatabaseCleaner();
  const command = process.argv[2];
  const days = parseInt(process.argv[3]) || 7;

  switch (command) {
    case 'old':
      cleaner.cleanOld(days);
      break;
    default:
      cleaner.clean();
  }
}

module.exports = DatabaseCleaner;
