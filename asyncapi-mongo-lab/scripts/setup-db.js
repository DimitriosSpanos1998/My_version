#!/usr/bin/env node

require('dotenv').config();
const MongoService = require('../src/services/mongo-service');
const AsyncAPIProcessor = require('../src/processors/asyncapi-processor');
const fs = require('fs-extra');
const path = require('path');

class DatabaseSetup {
  constructor() {
    this.mongoService = new MongoService();
    this.asyncapiProcessor = new AsyncAPIProcessor();
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

      // Import bundled AsyncAPI examples
      const importSummary = await this.importAsyncAPIExamples();

      console.log('\n📋 Setup Summary:');
      console.log('   - MongoDB connection established');
      console.log('   - Database indexes created');
      console.log('   - Logs directory created');
      console.log(`   - AsyncAPI examples imported: ${importSummary.inserted}`);
      if (importSummary.skipped > 0) {
        console.log(`   - Existing examples skipped: ${importSummary.skipped}`);
      }
      if (importSummary.errors.length > 0) {
        console.log(`   - Imports with errors: ${importSummary.errors.length}`);
      }

      console.log('\n✅ Database setup completed successfully!');

    } catch (error) {
      console.error('❌ Database setup failed:', error.message);
      process.exit(1);
    } finally {
      await this.mongoService.close();
    }
  }

  /**
   * Import AsyncAPI examples from the examples directory
   * @returns {Promise<{inserted: number, skipped: number, errors: Array}>} Import summary
   */
  async importAsyncAPIExamples() {
    const examplesDir = path.join(__dirname, '..', 'src', 'examples');
    const summary = {
      inserted: 0,
      skipped: 0,
      errors: []
    };

    if (!await fs.pathExists(examplesDir)) {
      console.log('ℹ️ No AsyncAPI examples directory found to import.');
      return summary;
    }

    console.log('\n📥 Importing bundled AsyncAPI examples...');

    const files = (await fs.readdir(examplesDir))
      .filter(file => /\.(json|ya?ml)$/i.test(file))
      .sort();

    if (files.length === 0) {
      console.log('ℹ️ No AsyncAPI example files detected.');
      return summary;
    }

    console.log(`📦 Found ${files.length} AsyncAPI example${files.length === 1 ? '' : 's'} in the examples directory.`);

    const normalizedCollection = this.mongoService.getCollection('normalized');

    for (const [index, file] of files.entries()) {
      const filePath = path.join(examplesDir, file);
      const extension = path.extname(file).replace('.', '').toLowerCase() || 'json';
      const progressLabel = `${index + 1}/${files.length}`;

      try {
        const processed = await this.asyncapiProcessor.processAsyncAPIFile(filePath, 'json');

        const alreadyImported = await normalizedCollection.findOne({
          'summary.title': processed.summary.title,
          'summary.version': processed.summary.version,
          'summary.protocol': processed.summary.protocol
        });
        if (alreadyImported) {
          summary.skipped += 1;
          console.log(`${progressLabel} ↩️  Skipping already imported example: ${file}`);
          continue;
        }

        await this.mongoService.insertAsyncAPIDocument({
          original: processed.original,
          normalized: processed.normalized,
          summary: processed.summary,
          searchableFields: processed.searchableFields
        });

        summary.inserted += 1;
        console.log(`${progressLabel} ✅ Imported AsyncAPI example: ${file}`);
      } catch (error) {
        summary.errors.push({ file, message: error.message });
        console.error(`${progressLabel} ❌ Failed to import ${file}:`, error.message);
      }
    }

    return summary;
  }

  /**
   * Clean database (remove all documents)
   */
  async clean() {
    try {
      console.log('🧹 Cleaning database...\n');

      await this.mongoService.connect();
      const normalizedCollection = this.mongoService.getCollection('normalized');
      const originalCollection = this.mongoService.getCollection('original');

      const [normalizedResult, originalResult] = await Promise.all([
        normalizedCollection.deleteMany({}),
        originalCollection.deleteMany({})
      ]);

      console.log(`🗑️ Deleted ${normalizedResult.deletedCount} normalized documents`);
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
      const originalCollection = this.mongoService.getCollection('original');

      const [normalizedCount, originalCount] = await Promise.all([
        normalizedCollection.countDocuments(),
        originalCollection.countDocuments()
      ]);

      console.log(`📈 Normalized documents: ${normalizedCount}`);
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
