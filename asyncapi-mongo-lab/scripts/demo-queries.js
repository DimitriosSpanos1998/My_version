#!/usr/bin/env node

require('dotenv').config();
const MongoService = require('../src/services/mongo-service');

class DemoQueries {
  constructor() {
    this.mongoService = new MongoService();
  }

  /**
   * Run demo queries
   */
  async run() {
    try {
      console.log('🔬 Running MongoDB Query Demonstrations...\n');

      await this.mongoService.connect();

      // Basic queries
      await this.basicQueries();

      // Advanced queries
      await this.advancedQueries();

      // Aggregation examples
      await this.aggregationExamples();

      console.log('\n✅ Demo queries completed!');
    } catch (error) {
      console.error('❌ Demo queries failed:', error.message);
      process.exit(1);
    } finally {
      await this.mongoService.close();
    }
  }

  /**
   * Basic query examples
   */
  async basicQueries() {
    console.log('📖 Basic Query Examples:');
    console.log('=' .repeat(40));

    // Find all documents
    console.log('\n1. Find all documents:');
    const allDocs = await this.mongoService.getAllAsyncAPIDocuments({ limit: 3 });
    console.log(`   Found ${allDocs.length} documents`);
    allDocs.forEach((doc, index) => {
      console.log(`   ${index + 1}. ${doc.metadata.title} (${doc.metadata.protocol})`);
    });

    // Find by protocol
    console.log('\n2. Find documents by protocol:');
    const protocols = ['mqtt', 'ws', 'websocket'];
    for (const protocol of protocols) {
      const docs = await this.mongoService.findDocumentsByProtocol(protocol);
      console.log(`   ${protocol}: ${docs.length} documents`);
    }

    // Search by text
    console.log('\n3. Search by text:');
    const searchTerms = ['user', 'sensor', 'device', 'api'];
    for (const term of searchTerms) {
      const results = await this.mongoService.searchAsyncAPIDocuments(term);
      console.log(`   "${term}": ${results.length} documents`);
    }

    // Find by version
    console.log('\n4. Find documents by version:');
    const versions = ['1.0.0', '2.1.0', '2.6.0'];
    for (const version of versions) {
      const docs = await this.mongoService.findDocumentsByVersion(version);
      console.log(`   v${version}: ${docs.length} documents`);
    }
  }

  /**
   * Advanced query examples
   */
  async advancedQueries() {
    console.log('\n🔬 Advanced Query Examples:');
    console.log('=' .repeat(40));

    const collection = this.mongoService.getCollection();

    // Complex queries
    console.log('\n1. Documents with multiple channels:');
    const multiChannelDocs = await this.mongoService.findAsyncAPIDocuments({
      'metadata.channelsCount': { $gt: 2 }
    });
    console.log(`   Found ${multiChannelDocs.length} documents with >2 channels`);

    console.log('\n2. Documents with specific server count:');
    const serverDocs = await this.mongoService.findAsyncAPIDocuments({
      'metadata.serversCount': { $gte: 2 }
    });
    console.log(`   Found ${serverDocs.length} documents with ≥2 servers`);

    console.log('\n3. Recent documents (last 24 hours):');
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const recentDocs = await this.mongoService.findAsyncAPIDocuments({
      'metadata.createdAt': { $gte: yesterday }
    });
    console.log(`   Found ${recentDocs.length} documents created in last 24 hours`);

    console.log('\n4. Documents with specific tags:');
    const taggedDocs = await this.mongoService.findAsyncAPIDocuments({
      'searchableFields.tags': { $in: ['iot', 'user', 'sensor'] }
    });
    console.log(`   Found ${taggedDocs.length} documents with specific tags`);

    console.log('\n5. Documents sorted by creation date:');
    const sortedDocs = await this.mongoService.findAsyncAPIDocuments(
      {},
      { sort: { 'metadata.createdAt': -1 }, limit: 3 }
    );
    console.log('   Most recent documents:');
    sortedDocs.forEach((doc, index) => {
      console.log(`     ${index + 1}. ${doc.metadata.title} (${doc.metadata.createdAt.toISOString()})`);
    });
  }

  /**
   * Aggregation examples
   */
  async aggregationExamples() {
    console.log('\n📊 Aggregation Examples:');
    console.log('=' .repeat(40));

    const collection = this.mongoService.getCollection();

    // Group by protocol
    console.log('\n1. Group by protocol:');
    const protocolStats = await collection.aggregate([
      {
        $group: {
          _id: '$metadata.protocol',
          count: { $sum: 1 },
          avgChannels: { $avg: '$metadata.channelsCount' },
          avgServers: { $avg: '$metadata.serversCount' }
        }
      },
      { $sort: { count: -1 } }
    ]).toArray();

    protocolStats.forEach(stat => {
      console.log(`   ${stat._id}: ${stat.count} docs, avg ${stat.avgChannels.toFixed(1)} channels`);
    });

    // Group by version
    console.log('\n2. Group by version:');
    const versionStats = await collection.aggregate([
      {
        $group: {
          _id: '$metadata.version',
          count: { $sum: 1 },
          protocols: { $addToSet: '$metadata.protocol' }
        }
      },
      { $sort: { count: -1 } }
    ]).toArray();

    versionStats.forEach(stat => {
      console.log(`   v${stat._id}: ${stat.count} docs, protocols: ${stat.protocols.join(', ')}`);
    });

    // Pipeline with multiple stages
    console.log('\n3. Complex aggregation pipeline:');
    const complexStats = await collection.aggregate([
      // Match documents with channels
      { $match: { 'metadata.channelsCount': { $gt: 0 } } },
      
      // Group by protocol
      {
        $group: {
          _id: '$metadata.protocol',
          totalChannels: { $sum: '$metadata.channelsCount' },
          totalServers: { $sum: '$metadata.serversCount' },
          documentCount: { $sum: 1 },
          titles: { $push: '$metadata.title' }
        }
      },
      
      // Add calculated fields
      {
        $addFields: {
          avgChannelsPerDoc: { $divide: ['$totalChannels', '$documentCount'] },
          avgServersPerDoc: { $divide: ['$totalServers', '$documentCount'] }
        }
      },
      
      // Sort by total channels
      { $sort: { totalChannels: -1 } }
    ]).toArray();

    complexStats.forEach(stat => {
      console.log(`   ${stat._id}:`);
      console.log(`     Documents: ${stat.documentCount}`);
      console.log(`     Total channels: ${stat.totalChannels}`);
      console.log(`     Avg channels/doc: ${stat.avgChannelsPerDoc.toFixed(1)}`);
      console.log(`     APIs: ${stat.titles.join(', ')}`);
    });

    // Date-based aggregation
    console.log('\n4. Documents by creation date:');
    const dateStats = await collection.aggregate([
      {
        $group: {
          _id: {
            year: { $year: '$metadata.createdAt' },
            month: { $month: '$metadata.createdAt' },
            day: { $dayOfMonth: '$metadata.createdAt' }
          },
          count: { $sum: 1 },
          protocols: { $addToSet: '$metadata.protocol' }
        }
      },
      { $sort: { '_id.year': -1, '_id.month': -1, '_id.day': -1 } },
      { $limit: 5 }
    ]).toArray();

    dateStats.forEach(stat => {
      const date = `${stat._id.year}-${stat._id.month.toString().padStart(2, '0')}-${stat._id.day.toString().padStart(2, '0')}`;
      console.log(`   ${date}: ${stat.count} docs (${stat.protocols.join(', ')})`);
    });
  }
}

// Run demo if this file is executed directly
if (require.main === module) {
  const demo = new DemoQueries();
  demo.run().catch(console.error);
}

module.exports = DemoQueries;
