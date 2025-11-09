#!/usr/bin/env node

require('dotenv').config();
const MongoService = require('../src/services/mongo-service');
const fs = require('fs-extra');
const path = require('path');

class DatabaseSetup {
  constructor() {
    this.mongoService = new MongoService();
  }

  /**
   * Setup database and create indexes
   */
  async setup() {
    try {
      console.log('🔧 Setting up MongoDB database...\n');

      // Connect to database
      await this.mongoService.connect();

      // Create indexes for better performance
      console.log('📊 Creating database indexes...');
      const databaseConfig = require('../src/config/database');
      await databaseConfig.createIndexes();

      // Create logs directory
      await fs.ensureDir('logs');
      console.log('📁 Created logs directory');

      // Test database connection
      console.log('🔍 Testing database connection...');
      const testDoc = {
        metadata: {
          title: 'Test Document',
          version: '1.0.0',
          description: 'Test document for setup verification',
          protocol: 'test',
          channelsCount: 0,
          serversCount: 0,
          createdAt: new Date(),
          updatedAt: new Date(),
          processedAt: new Date()
        },
        searchableFields: {
          title: 'test document',
          description: 'test document for setup verification',
          version: '1.0.0',
          protocol: 'test',
          tags: ['test', 'setup']
        }
      };

      const result = await this.mongoService.insertAsyncAPIDocument({
        original: JSON.stringify(testDoc, null, 2),
        normalized: testDoc
      });
      console.log(`✅ Test document inserted with ID: ${result.insertedId}`);

      // Clean up test document
      await this.mongoService.deleteAsyncAPIDocument(result.insertedId.toString());
      console.log('🧹 Test document cleaned up');

      console.log('\n✅ Database setup completed successfully!');
      console.log('\n📋 Setup Summary:');
      console.log('   - MongoDB connection established');
      console.log('   - Database indexes created');
      console.log('   - Logs directory created');
      console.log('   - Connection test passed');

    } catch (error) {
      console.error('❌ Database setup failed:', error.message);
      process.exit(1);
    } finally {
      await this.mongoService.close();
    }
  }

  /**
   * Clean database (remove all documents)
   */
  async clean() {
    try {
      console.log('🧹 Cleaning database...\n');

      await this.mongoService.connect();
      const normalizedCollection = this.mongoService.getCollection('normalized');
      const metadataCollection = this.mongoService.getCollection('metadata');
      const originalCollection = this.mongoService.getCollection('original');

      const [normalizedResult, metadataResult, originalResult] = await Promise.all([
        normalizedCollection.deleteMany({}),
        metadataCollection.deleteMany({}),
        originalCollection.deleteMany({})
      ]);

      console.log(`🗑️ Deleted ${normalizedResult.deletedCount} normalized documents`);
      console.log(`🗑️ Deleted ${metadataResult.deletedCount} metadata documents`);
      console.log(`🗑️ Deleted ${originalResult.deletedCount} original documents`);

      console.log('✅ Database cleaned successfully!');
    } catch (error) {
      console.error('❌ Database cleanup failed:', error.message);
      process.exit(1);
    } finally {
      await this.mongoService.close();
    }
  }

  /**
   * Show database status
   */
  async status() {
    try {
      console.log('📊 Database Status:\n');

      await this.mongoService.connect();
      const normalizedCollection = this.mongoService.getCollection('normalized');
      const metadataCollection = this.mongoService.getCollection('metadata');
      const originalCollection = this.mongoService.getCollection('original');

      const [normalizedCount, metadataCount, originalCount] = await Promise.all([
        normalizedCollection.countDocuments(),
        metadataCollection.countDocuments(),
        originalCollection.countDocuments()
      ]);

      console.log(`📈 Normalized documents: ${normalizedCount}`);
      console.log(`📈 Metadata documents: ${metadataCount}`);
      console.log(`📈 Original documents: ${originalCount}`);

      if (normalizedCount > 0) {
        const stats = await this.mongoService.getDocumentStatistics();

        console.log('\n🌐 Protocol distribution:');
        stats.protocolDistribution.forEach(protocol => {
          console.log(`   ${protocol._id}: ${protocol.count}`);
        });

        console.log('\n📋 Version distribution:');
        stats.versionDistribution.forEach(version => {
          console.log(`   v${version._id}: ${version.count}`);
        });
      }

    } catch (error) {
      console.error('❌ Error getting database status:', error.message);
      process.exit(1);
    } finally {
      await this.mongoService.close();
    }
  }
}

// Run setup if this file is executed directly
if (require.main === module) {
  const setup = new DatabaseSetup();
  const command = process.argv[2];

  switch (command) {
    case 'clean':
      setup.clean();
      break;
    case 'status':
      setup.status();
      break;
    default:
      setup.setup();
  }
}

module.exports = DatabaseSetup;
