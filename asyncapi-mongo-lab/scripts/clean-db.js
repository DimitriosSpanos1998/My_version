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
      const originalCollection = this.mongoService.getCollection('original');
      const metadaCollection = this.mongoService.getCollection('metada');

      const [normalizedBefore, originalBefore, metadaBefore] = await Promise.all([
        normalizedCollection.countDocuments(),
        originalCollection.countDocuments(),
        metadaCollection.countDocuments()
      ]);

      console.log(`📊 Normalized before cleanup: ${normalizedBefore}`);
      console.log(`📊 Original before cleanup: ${originalBefore}`);
      console.log(`📊 Metada before cleanup: ${metadaBefore}`);

      if (normalizedBefore === 0 && originalBefore === 0 && metadaBefore === 0) {
        console.log('✅ Database is already clean!');
        return;
      }

      const [normalizedResult, originalResult, metadaResult] = await Promise.all([
        normalizedCollection.deleteMany({}),
        originalCollection.deleteMany({}),
        metadaCollection.deleteMany({})
      ]);

      console.log(`🗑️ Deleted ${normalizedResult.deletedCount} normalized documents`);
      console.log(`🗑️ Deleted ${originalResult.deletedCount} original documents`);
      console.log(`🗑️ Deleted ${metadaResult.deletedCount} metada documents`);

      const [normalizedAfter, originalAfter, metadaAfter] = await Promise.all([
        normalizedCollection.countDocuments(),
        originalCollection.countDocuments(),
        metadaCollection.countDocuments()
      ]);

      console.log(`📊 Normalized after cleanup: ${normalizedAfter}`);
      console.log(`📊 Original after cleanup: ${originalAfter}`);
      console.log(`📊 Metada after cleanup: ${metadaAfter}`);

      if (normalizedAfter === 0 && originalAfter === 0 && metadaAfter === 0) {
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
      const originalCollection = this.mongoService.getCollection('original');
      const metadaCollection = this.mongoService.getCollection('metada');

      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - days);

      const normalizedFilter = { 'summary.createdAt': { $lt: cutoffDate } };

      const normalizedBefore = await normalizedCollection.countDocuments(normalizedFilter);
      console.log(`📊 Normalized docs older than ${days} days: ${normalizedBefore}`);

      if (normalizedBefore === 0) {
        console.log('✅ No old documents to clean!');
        return;
      }

      const docsToDelete = await normalizedCollection
        .find(normalizedFilter, { projection: { _id: 1 } })
        .toArray();

      const normalizedIds = docsToDelete.map(doc => doc._id);

      const [normalizedResult, originalResult, metadaResult] = await Promise.all([
        normalizedCollection.deleteMany({ _id: { $in: normalizedIds } }),
        normalizedIds.length
          ? originalCollection.deleteMany({ normalizedId: { $in: normalizedIds } })
          : Promise.resolve({ deletedCount: 0 }),
        normalizedIds.length
          ? metadaCollection.deleteMany({ normalizedId: { $in: normalizedIds } })
          : Promise.resolve({ deletedCount: 0 })
      ]);

      console.log(`🗑️ Deleted ${normalizedResult.deletedCount} normalized documents`);
      console.log(`🗑️ Deleted ${originalResult.deletedCount || 0} original documents`);
      console.log(`🗑️ Deleted ${metadaResult.deletedCount || 0} metada documents`);

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
