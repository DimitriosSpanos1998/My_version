# AsyncAPI MongoDB Lab

A hands-on learning project that demonstrates MongoDB operations through AsyncAPI specification processing. This lab provides practical experience with document databases, CRUD operations, and real-world data management using AsyncAPI specifications.

## 🎯 Project Overview

This project combines two powerful technologies:
- **AsyncAPI**: Industry standard for defining asynchronous APIs
- **MongoDB**: Popular document-based NoSQL database

You'll learn how to:
- Process and convert AsyncAPI specifications
- Store structured API data in MongoDB
- Perform CRUD operations (Create, Read, Update, Delete)
- Execute complex queries and aggregations
- Understand document database patterns

## 🚀 Quick Start

### Prerequisites

- **Node.js** (v16 or higher)
- **MongoDB** (v5.0 or higher) - local installation or Atlas account
- **Git** for version control

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd asyncapi-mongo-lab
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Setup environment**
   ```bash
   # Create .env file with your MongoDB connection
   echo "MONGODB_URI=mongodb://localhost:27017/asyncapi-lab" > .env
   echo "DB_NAME=asyncapi-lab" >> .env
   echo "NORMALIZED_COLLECTION_NAME=asyncapi_normalized" >> .env
   echo "METADATA_COLLECTION_NAME=asyncapi_metadata" >> .env
   echo "ORIGINAL_COLLECTION_NAME=asyncapi_originals" >> .env
   ```

4. **Setup database**
   ```bash
   npm run setup
   ```

5. **Run the application**
   ```bash
   npm start
   ```

## 📁 Project Structure

```
asyncapi-mongo-lab/
├── package.json                 # Project dependencies and scripts
├── .env                        # Environment variables (MongoDB connection)
├── .gitignore                  # Git ignore file
├── README.md                   # This file
├── src/
│   ├── config/
│   │   └── database.js         # MongoDB connection configuration
│   ├── processors/
│   │   └── asyncapi-processor.js # AsyncAPI conversion logic
│   ├── services/
│   │   └── mongo-service.js    # MongoDB CRUD operations
│   ├── examples/
│   │   ├── sample-asyncapi.yaml # Sample AsyncAPI file (WebSocket)
│   │   └── sample-asyncapi.json # Sample AsyncAPI file (MQTT)
│   ├── utils/
│   │   └── logger.js           # Logging utility
│   └── index.js                # Main application entry point
├── scripts/
│   ├── setup-db.js             # Database setup script
│   ├── demo-queries.js         # Demo query examples
│   └── clean-db.js             # Database cleanup script
└── tests/
    ├── asyncapi-processor.test.js
    └── mongo-service.test.js
```

## 🛠️ Available Scripts

| Script | Description |
|--------|-------------|
| `npm start` | Run the main application |
| `npm run dev` | Run in development mode with auto-restart |
| `npm run setup` | Setup database and create indexes |
| `npm run demo` | Run demo queries and aggregations |
| `npm run clean` | Clean all documents from database |
| `npm test` | Run test suite |

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# MongoDB Connection
MONGODB_URI=mongodb://localhost:27017/asyncapi-lab
DB_NAME=asyncapi-lab
NORMALIZED_COLLECTION_NAME=asyncapi_normalized
METADATA_COLLECTION_NAME=asyncapi_metadata
ORIGINAL_COLLECTION_NAME=asyncapi_originals
# Optional legacy fallback
COLLECTION_NAME=asyncapi_normalized

# Optional: MongoDB Atlas connection
# MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/asyncapi-lab?retryWrites=true&w=majority

# Logging
LOG_LEVEL=info
```

### Collection Layout

The lab uses three MongoDB collections to separate different representations of each AsyncAPI document:

- **Normalized** (`NORMALIZED_COLLECTION_NAME`): Stores the enriched, query-ready document (including metadata and searchable fields).
- **Metadata** (`METADATA_COLLECTION_NAME`): Stores a dedicated metadata document for fast lookups and cross-collection references.
- **Original** (`ORIGINAL_COLLECTION_NAME`): Stores the raw AsyncAPI source content for auditing or re-processing purposes.

Each insert operation automatically writes to all three collections and keeps the metadata document synchronized during updates and deletions.

### MongoDB Setup Options

**Option 1: Local MongoDB**
```bash
# Install MongoDB locally
brew install mongodb/brew/mongodb-community  # macOS
# or
sudo apt-get install mongodb                 # Ubuntu

# Start MongoDB service
brew services start mongodb/brew/mongodb-community  # macOS
# or
sudo systemctl start mongod                  # Ubuntu
```

**Option 2: MongoDB Atlas (Cloud)**
1. Create a free account at [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
2. Create a cluster
3. Get your connection string
4. Update `MONGODB_URI` in `.env`

## 📚 Learning Modules

### 1. AsyncAPI Processing

The `AsyncAPIProcessor` class handles:
- Loading AsyncAPI files (YAML/JSON)
- Parsing and validation
- Format conversion
- Data normalization for MongoDB

```javascript
const processor = new AsyncAPIProcessor();
const result = await processor.processAsyncAPIFile('path/to/asyncapi.yaml');
```

### 2. MongoDB Operations

The `MongoService` class provides:
- Document insertion
- Query operations
- Updates and deletions
- Search functionality
- Statistics and aggregations

```javascript
const mongoService = new MongoService();
await mongoService.connect();

// Insert document across original, normalized, and metadata collections
const result = await mongoService.insertAsyncAPIDocument({
  original: JSON.stringify(asyncAPISpec, null, 2),
  normalized: normalizedData
});

// Find documents
const docs = await mongoService.findDocumentsByProtocol('mqtt');

// Search
const results = await mongoService.searchAsyncAPIDocuments('user');
```

### 3. CRUD Operations

**Create (Insert)**
```javascript
const insertResult = await mongoService.insertAsyncAPIDocument({
  original: asyncAPIContentAsString,
  normalized: asyncAPIData
});
```

**Read (Find)**
```javascript
// Find all
const allDocs = await mongoService.getAllAsyncAPIDocuments();

// Find by criteria
const mqttDocs = await mongoService.findDocumentsByProtocol('mqtt');

// Search by text
const userDocs = await mongoService.searchAsyncAPIDocuments('user');
```

**Update**
```javascript
const updateResult = await mongoService.updateAsyncAPIDocument(
  documentId,
  { 'metadata.description': 'Updated description' }
);
```

**Delete**
```javascript
const deleteResult = await mongoService.deleteAsyncAPIDocument(documentId);
```

## 🔍 Query Examples

### Basic Queries

```javascript
// Find by protocol
const wsDocs = await mongoService.findDocumentsByProtocol('ws');

// Find by version
const v1Docs = await mongoService.findDocumentsByVersion('1.0.0');

// Search by text
const searchResults = await mongoService.searchAsyncAPIDocuments('sensor');
```

### Advanced Queries

```javascript
// Complex criteria
const complexDocs = await mongoService.findAsyncAPIDocuments({
  'metadata.channelsCount': { $gt: 2 },
  'metadata.serversCount': { $gte: 1 }
});

// Date range
const recentDocs = await mongoService.findAsyncAPIDocuments({
  'metadata.createdAt': { $gte: new Date('2023-01-01') }
});
```

### Aggregation Examples

```javascript
// Group by protocol
const protocolStats = await collection.aggregate([
  {
    $group: {
      _id: '$metadata.protocol',
      count: { $sum: 1 },
      avgChannels: { $avg: '$metadata.channelsCount' }
    }
  },
  { $sort: { count: -1 } }
]).toArray();
```

## 📊 Sample Data

The project includes two sample AsyncAPI specifications:

1. **User Service API** (`sample-asyncapi.yaml`)
   - WebSocket protocol
   - User management operations
   - Real-time notifications

2. **MQTT Broker API** (`sample-asyncapi.json`)
   - MQTT protocol
   - IoT device communication
   - Sensor data handling

## 🧪 Testing

Run the test suite:
```bash
npm test
```

The tests cover:
- AsyncAPI processing functionality
- MongoDB service operations
- Error handling scenarios

## 🚨 Troubleshooting

### Common Issues

**MongoDB Connection Error**
```
❌ MongoDB connection error: connect ECONNREFUSED 127.0.0.1:27017
```
- Ensure MongoDB is running locally
- Check your connection string in `.env`
- Verify MongoDB service status

**AsyncAPI Parsing Error**
```
❌ Error parsing AsyncAPI: Invalid AsyncAPI document
```
- Check AsyncAPI file format
- Ensure file is valid YAML/JSON
- Verify AsyncAPI version compatibility

**Permission Errors**
```
❌ Error: EACCES permission denied
```
- Check file permissions
- Ensure MongoDB user has proper access
- Verify database connection credentials

### Debug Mode

Enable debug logging:
```bash
LOG_LEVEL=debug npm start
```

## 📈 Performance Tips

1. **Use Indexes**: The setup script creates indexes for common query patterns
2. **Limit Results**: Use `limit` option for large result sets
3. **Project Fields**: Use projection to return only needed fields
4. **Batch Operations**: Use bulk operations for multiple documents

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [AsyncAPI](https://www.asyncapi.com/) for the specification standard
- [MongoDB](https://www.mongodb.com/) for the database technology
- [Node.js](https://nodejs.org/) for the runtime environment

## 📞 Support

If you encounter any issues or have questions:
1. Check the troubleshooting section
2. Review the code examples
3. Open an issue on GitHub
4. Check the AsyncAPI and MongoDB documentation

---

**Happy Learning! 🎉**

This lab provides a solid foundation for understanding both AsyncAPI processing and MongoDB operations. Experiment with the code, modify the sample data, and explore different query patterns to deepen your understanding.