#!/usr/bin/env node

require('dotenv').config();
const AsyncAPIProcessor = require('./processors/asyncapi-processor');
const MongoService = require('./services/mongo-service');
const path = require('path');

class AsyncAPIMongoLab {
  constructor() {
    this.processor = new AsyncAPIProcessor();
    this.mongoService = new MongoService();
  }

  /**
   * Main application entry point
   */
  async run() {
    try {
      console.log('🚀 Starting AsyncAPI MongoDB Lab...\n');

      // Connect to MongoDB
      await this.mongoService.connect();
      const databaseConfig = require('./config/database');
      await databaseConfig.createIndexes();

      // Process sample AsyncAPI files
      await this.processSampleFiles();

      // Demonstrate CRUD operations
      await this.demonstrateCRUDOperations();

      // Show statistics
      await this.showStatistics();

      console.log('\n✅ AsyncAPI MongoDB Lab completed successfully!');
    } catch (error) {
      console.error('❌ Application error:', error.message);
      process.exit(1);
    } finally {
      await this.mongoService.close();
    }
  }

  /**
   * Process sample AsyncAPI files
   */
  async processSampleFiles() {
    console.log('📄 Processing sample AsyncAPI files...\n');

    const sampleFiles = [
      'src/examples/sample-asyncapi.yaml',
      'src/examples/sample-asyncapi.json'
    ];

    for (const filePath of sampleFiles) {
      try {
        console.log(`\n🔄 Processing: ${filePath}`);
        
        // Process the AsyncAPI file
        const result = await this.processor.processAsyncAPIFile(filePath, 'json');

        // Insert into MongoDB (normalized summary plus original documents)
        const insertResult = await this.mongoService.insertAsyncAPIDocument(result);

        console.log(`✅ Successfully processed and stored: ${result.summary.title} v${result.summary.version}`);
        console.log(`   Document ID: ${insertResult.insertedId}`);
        if (insertResult.originalId) {
          console.log(`   Original ID: ${insertResult.originalId}`);
        }
        console.log(`   Protocol: ${result.summary.protocol}`);
        console.log(`   Channels: ${result.summary.channelsCount}`);
        console.log(`   Servers: ${result.summary.serversCount}`);
        
      } catch (error) {
        console.error(`❌ Error processing ${filePath}:`, error.message);
      }
    }
  }

  /**
   * Demonstrate CRUD operations
   */
  async demonstrateCRUDOperations() {
    console.log('\n🔧 Demonstrating CRUD operations...\n');

    // READ: Find all documents
    console.log('📖 READ: Finding all documents...');
    const allDocs = await this.mongoService.getAllAsyncAPIDocuments({ limit: 5 });
    console.log(`   Found ${allDocs.length} documents`);

    if (allDocs.length > 0) {
      const firstDoc = allDocs[0];
      console.log(`   First document: ${firstDoc.summary.title} (ID: ${firstDoc._id})`);

      // READ: Find by protocol
      console.log('\n🔍 READ: Finding documents by protocol...');
      const mqttDocs = await this.mongoService.findDocumentsByProtocol('mqtt');
      console.log(`   Found ${mqttDocs.length} MQTT documents`);

      const wsDocs = await this.mongoService.findDocumentsByProtocol('ws');
      console.log(`   Found ${wsDocs.length} WebSocket documents`);

      // READ: Search by text
      console.log('\n🔍 READ: Searching by text...');
      const searchResults = await this.mongoService.searchAsyncAPIDocuments('user');
      console.log(`   Found ${searchResults.length} documents containing 'user'`);

      // UPDATE: Update a document
      console.log('\n✏️ UPDATE: Updating document...');
      const updateResult = await this.mongoService.updateAsyncAPIDocument(
        firstDoc._id.toString(),
        {
          'summary.description': 'Updated description via MongoDB lab',
          'summary.tags': ['updated', 'mongodb-lab']
        }
      );
      console.log(`   Updated ${updateResult.modifiedCount} field(s)`);

      // READ: Verify update
      console.log('\n🔍 READ: Verifying update...');
      const updatedDoc = await this.mongoService.findAsyncAPIDocumentById(firstDoc._id.toString());
      console.log(`   Updated description: ${updatedDoc.summary.description}`);

      // DELETE: Delete a document (only if we have more than one)
      if (allDocs.length > 1) {
        console.log('\n🗑️ DELETE: Deleting a document...');
        const deleteResult = await this.mongoService.deleteAsyncAPIDocument(firstDoc._id.toString());
        console.log(`   Deleted ${deleteResult.deletedCount} document(s)`);

        // READ: Verify deletion
        console.log('\n🔍 READ: Verifying deletion...');
        const remainingDocs = await this.mongoService.getAllAsyncAPIDocuments();
        console.log(`   Remaining documents: ${remainingDocs.length}`);
      } else {
        console.log('\n⚠️ SKIP DELETE: Only one document remaining, skipping deletion');
      }
    }
  }

  /**
   * Show database statistics
   */
  async showStatistics() {
    console.log('\n📊 Database Statistics:');
    console.log('=' .repeat(50));

    try {
      const stats = await this.mongoService.getDocumentStatistics();
      
      console.log(`📈 Total Documents: ${stats.totalDocuments}`);
      
      console.log('\n🌐 Protocol Distribution:');
      stats.protocolDistribution.forEach(protocol => {
        console.log(`   ${protocol._id}: ${protocol.count} document(s)`);
      });

      console.log('\n📋 Version Distribution:');
      stats.versionDistribution.forEach(version => {
        console.log(`   v${version._id}: ${version.count} document(s)`);
      });

      console.log(`\n🕒 Last Updated: ${stats.lastUpdated.toISOString()}`);
    } catch (error) {
      console.error('❌ Error getting statistics:', error.message);
    }
  }

  /**
   * Demonstrate advanced queries
   */
  async demonstrateAdvancedQueries() {
    console.log('\n🔬 Advanced Query Demonstrations:');
    console.log('=' .repeat(50));

    try {
      // Find documents with specific criteria
      console.log('\n🔍 Finding documents with channels > 2...');
      const complexQuery = await this.mongoService.findAsyncAPIDocuments({
        'summary.channelsCount': { $gt: 2 }
      });
      console.log(`   Found ${complexQuery.length} documents with more than 2 channels`);

      // Find documents created today
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      console.log('\n🔍 Finding documents created today...');
      const todayDocs = await this.mongoService.findAsyncAPIDocuments({
        'summary.createdAt': { $gte: today }
      });
      console.log(`   Found ${todayDocs.length} documents created today`);

      // Aggregate operations
      console.log('\n📊 Aggregating data...');
      const collection = this.mongoService.getCollection();
      const aggregationResult = await collection.aggregate([
        {
          $group: {
            _id: '$summary.protocol',
            avgChannels: { $avg: '$summary.channelsCount' },
            avgServers: { $avg: '$summary.serversCount' },
            count: { $sum: 1 }
          }
        },
        { $sort: { count: -1 } }
      ]).toArray();

      console.log('   Protocol Statistics:');
      aggregationResult.forEach(stat => {
        console.log(`     ${stat._id}: ${stat.count} docs, avg ${stat.avgChannels.toFixed(1)} channels, avg ${stat.avgServers.toFixed(1)} servers`);
      });

    } catch (error) {
      console.error('❌ Error in advanced queries:', error.message);
    }
  }
}

// Run the application if this file is executed directly
if (require.main === module) {
  const app = new AsyncAPIMongoLab();
  app.run().catch(console.error);
}

module.exports = AsyncAPIMongoLab;
