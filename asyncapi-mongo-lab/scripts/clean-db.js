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
      const collection = this.mongoService.getCollection();
      
      // Get count before deletion
      const countBefore = await collection.countDocuments();
      console.log(`📊 Documents before cleanup: ${countBefore}`);

      if (countBefore === 0) {
        console.log('✅ Database is already clean!');
        return;
      }

      // Delete all documents
      const result = await collection.deleteMany({});
      console.log(`🗑️ Deleted ${result.deletedCount} documents`);

      // Verify cleanup
      const countAfter = await collection.countDocuments();
      console.log(`📊 Documents after cleanup: ${countAfter}`);

      if (countAfter === 0) {
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
      const collection = this.mongoService.getCollection();
      
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - days);

      // Get count before deletion
      const countBefore = await collection.countDocuments({
        'metadata.createdAt': { $lt: cutoffDate }
      });
      console.log(`📊 Documents older than ${days} days: ${countBefore}`);

      if (countBefore === 0) {
        console.log('✅ No old documents to clean!');
        return;
      }

      // Delete old documents
      const result = await collection.deleteMany({
        'metadata.createdAt': { $lt: cutoffDate }
      });
      console.log(`🗑️ Deleted ${result.deletedCount} old documents`);

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
