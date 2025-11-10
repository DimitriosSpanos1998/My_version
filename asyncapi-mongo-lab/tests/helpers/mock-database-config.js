const { ObjectId } = require('mongodb');

const clone = (value) => JSON.parse(JSON.stringify(value));

const getValueByPath = (object, path) => {
  if (!path) return undefined;
  return path.split('.').reduce((acc, key) => (acc == null ? undefined : acc[key]), object);
};

const matchesValue = (value, condition) => {
  if (condition && typeof condition === 'object') {
    if ('$regex' in condition) {
      const regex = new RegExp(condition.$regex, condition.$options || '');
      if (Array.isArray(value)) {
        return value.some((item) => regex.test(String(item)));
      }
      return regex.test(String(value ?? ''));
    }
  }
  return value === condition;
};

const documentMatches = (document, filter = {}) => {
  if (document == null) {
    return false;
  }
  if (!filter || Object.keys(filter).length === 0) {
    return true;
  }

  if (filter.$or) {
    return filter.$or.some((subFilter) => documentMatches(document, subFilter));
  }

  return Object.entries(filter).every(([key, value]) => {
    if (key === '_id' && value instanceof ObjectId) {
      return document._id?.toString() === value.toString();
    }

    if (typeof value === 'object' && !Array.isArray(value) && !('$regex' in value)) {
      const docValue = document[key];
      return documentMatches(docValue, value);
    }

    const docValue = key.includes('.') ? getValueByPath(document, key) : document[key];
    return matchesValue(docValue, value);
  });
};

class MockCursor {
  constructor(documents) {
    this.documents = documents;
    this._sort = null;
    this._limit = null;
    this._projection = null;
  }

  sort(sort) {
    this._sort = sort;
    return this;
  }

  limit(limit) {
    this._limit = limit;
    return this;
  }

  project(projection) {
    this._projection = projection;
    return this;
  }

  toArray() {
    let results = [...this.documents];

    if (this._sort) {
      const entries = Object.entries(this._sort);
      results.sort((a, b) => {
        for (const [path, direction] of entries) {
          const aValue = getValueByPath(a, path);
          const bValue = getValueByPath(b, path);
          if (aValue === bValue) continue;
          if (aValue == null) return 1;
          if (bValue == null) return -1;
          if (aValue > bValue) return direction < 0 ? -1 : 1;
          if (aValue < bValue) return direction < 0 ? 1 : -1;
        }
        return 0;
      });
    }

    if (typeof this._limit === 'number') {
      results = results.slice(0, this._limit);
    }

    if (this._projection) {
      results = results.map((doc) => applyProjection(doc, this._projection));
    }

    return results.map((doc) => clone(doc));
  }
}

const applyProjection = (document, projection = {}) => {
  const includeKeys = Object.entries(projection)
    .filter(([, value]) => value)
    .map(([key]) => key);

  if (includeKeys.length === 0) {
    return clone(document);
  }

  const projected = {};
  includeKeys.forEach((key) => {
    if (key === '_id') {
      projected._id = document._id;
    } else {
      projected[key] = getValueByPath(document, key);
    }
  });

  return projected;
};

class MockCollection {
  constructor(name) {
    this.name = name;
    this.documents = [];
  }

  async insertOne(document) {
    const inserted = clone(document);
    inserted._id = inserted._id ? new ObjectId(inserted._id) : new ObjectId();
    this.documents.push(inserted);
    return { acknowledged: true, insertedId: inserted._id };
  }

  async deleteOne(filter = {}) {
    const index = this.documents.findIndex((doc) => documentMatches(doc, filter));
    if (index === -1) {
      return { acknowledged: true, deletedCount: 0 };
    }
    this.documents.splice(index, 1);
    return { acknowledged: true, deletedCount: 1 };
  }

  async deleteMany(filter = {}) {
    const before = this.documents.length;
    this.documents = this.documents.filter((doc) => !documentMatches(doc, filter));
    return { acknowledged: true, deletedCount: before - this.documents.length };
  }

  async findOne(filter = {}, options = {}) {
    const document = this.documents.find((doc) => documentMatches(doc, filter));
    if (!document) return null;
    const cloned = clone(document);
    if (options?.projection) {
      return applyProjection(cloned, options.projection);
    }
    return cloned;
  }

  find(filter = {}) {
    const matched = this.documents.filter((doc) => documentMatches(doc, filter));
    return new MockCursor(matched);
  }

  async updateOne(filter = {}, update = {}) {
    const document = this.documents.find((doc) => documentMatches(doc, filter));
    if (!document) {
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0 };
    }

    if (update?.$set) {
      Object.entries(update.$set).forEach(([path, value]) => {
        const parts = path.split('.');
        let target = document;
        while (parts.length > 1) {
          const key = parts.shift();
          target[key] = target[key] ?? {};
          target = target[key];
        }
        target[parts[0]] = value;
      });
    }

    return { acknowledged: true, matchedCount: 1, modifiedCount: 1 };
  }

  async updateMany(filter = {}, update = {}) {
    let modifiedCount = 0;
    for (const document of this.documents) {
      if (!documentMatches(document, filter)) continue;
      if (update?.$set) {
        Object.entries(update.$set).forEach(([path, value]) => {
          const parts = path.split('.');
          let target = document;
          while (parts.length > 1) {
            const key = parts.shift();
            target[key] = target[key] ?? {};
            target = target[key];
          }
          target[parts[0]] = value;
        });
      }
      modifiedCount += 1;
    }
    return { acknowledged: true, matchedCount: modifiedCount, modifiedCount };
  }

  async dropIndexes() {
    return { ok: 1 };
  }

  async createIndex() {
    return 'mock_index';
  }
}

class MockDatabaseConfig {
  constructor() {
    this.collections = new Map();
    this.connected = false;
  }

  getCollectionNames() {
    return {
      original: 'original',
      normalized: 'normalized'
    };
  }

  getCollectionName(type = 'normalized') {
    const names = this.getCollectionNames();
    return names[type] || type;
  }

  async connect() {
    this.connected = true;
    return {
      collection: (name) => this.getCollection(name)
    };
  }

  getCollection(type = 'normalized') {
    const name = this.getCollectionName(type);
    if (!this.collections.has(name)) {
      this.collections.set(name, new MockCollection(name));
    }
    return this.collections.get(name);
  }

  async close() {
    this.connected = false;
    this.collections.clear();
  }

  isDatabaseConnected() {
    return this.connected;
  }

  async createIndexes() {
    return true;
  }

  async createNormalizedIndexes() {
    return true;
  }

  async createOriginalIndexes() {
    return true;
  }

  async dropCollectionIndexes() {
    return true;
  }
}

const createMockDatabaseConfig = () => new MockDatabaseConfig();

module.exports = {
  MockDatabaseConfig,
  createMockDatabaseConfig
};
