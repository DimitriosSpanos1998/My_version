#!/usr/bin/env node

require('dotenv').config();
const MongoService = require('../src/services/mongo-service');

async function dropIndexes() {
  try {
    console.log('🗑️ Dropping all indexes...\n');
    
    const mongoService = new MongoService();
    await mongoService.connect();
    
    const collection = mongoService.getCollection();
    const indexes = await collection.indexes();
    
    console.log('Current indexes:');
    indexes.forEach(idx => {
      console.log(`  - ${idx.name}: ${JSON.stringify(idx.key)}`);
    });
    
    // Drop all indexes except _id
    await collection.dropIndexes();
    console.log('\n✅ All indexes dropped successfully (except _id)');
    
    console.log('\n📋 Remaining indexes:');
    const remainingIndexes = await collection.indexes();
    remainingIndexes.forEach(idx => {
      console.log(`  - ${idx.name}: ${JSON.stringify(idx.key)}`);
    });
    
    console.log('\n💡 You can now run "npm run setup" to recreate indexes');
    
  } catch (error) {
    console.error('❌ Error dropping indexes:', error.message);
    process.exit(1);
  }
}

dropIndexes().catch(console.error);
