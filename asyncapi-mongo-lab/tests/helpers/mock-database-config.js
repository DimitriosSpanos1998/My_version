const { ObjectId } = require('mongodb');

const cloneValue = value => {
  if (value instanceof ObjectId) {
    return new ObjectId(value.toHexString());
  }
  if (value instanceof Date) {
    return new Date(value.getTime());
  }
  if (Array.isArray(value)) {
    return value.map(cloneValue);
  }
  if (value && typeof value === 'object') {
    return Object.entries(value).reduce((acc, [key, val]) => {
      acc[key] = cloneValue(val);
      return acc;
    }, {});
  }
  return value;
};

const getValueByPath = (obj, path) => {
  if (!path) return undefined;
  return path.split('.').reduce((current, segment) => {
    if (current == null) {
      return undefined;
    }
    return current[segment];
  }, obj);
};

const setValueByPath = (obj, path, value) => {
  const segments = path.split('.');
  let current = obj;
  segments.forEach((segment, index) => {
    if (index === segments.length - 1) {
      current[segment] = value;
    } else {
      if (!current[segment] || typeof current[segment] !== 'object') {
        current[segment] = {};
      }
      current = current[segment];
    }
  });
};

const equals = (a, b) => {
  if (a instanceof ObjectId && b instanceof ObjectId) {
    return a.toHexString() === b.toHexString();
  }
  if (a instanceof ObjectId && typeof b === 'string') {
    return a.toHexString() === b;
  }
  if (b instanceof ObjectId && typeof a === 'string') {
    return b.toHexString() === a;
  }
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }
  if (a instanceof Date && typeof b === 'string') {
    return a.toISOString() === b;
  }
  if (b instanceof Date && typeof a === 'string') {
    return b.toISOString() === a;
  }
  return a === b;
};

const matchesCondition = (value, condition) => {
  const isOperatorObject =
    condition &&
    typeof condition === 'object' &&
    !Array.isArray(condition) &&
    !(condition instanceof Date) &&
    !(condition instanceof ObjectId);

  if (!isOperatorObject) {
    return equals(value, condition);
  }

  return Object.entries(condition).every(([operator, expected]) => {
    switch (operator) {
      case '$gt':
        return value > expected;
      case '$gte':
        return value >= expected;
      case '$lt':
        return value < expected;
      case '$lte':
        return value <= expected;
      case '$in': {
        const list = Array.isArray(expected) ? expected : [expected];
        if (Array.isArray(value)) {
          return value.some(item => list.some(target => equals(item, target)));
        }
        return list.some(item => equals(value, item));
      }
      case '$regex': {
        if (value === undefined || value === null) {
          return false;
        }
        const flags = condition.$options || '';
        const regex = expected instanceof RegExp ? expected : new RegExp(expected, flags);
        if (Array.isArray(value)) {
          return value.some(item => regex.test(String(item)));
        }
        return regex.test(String(value));
      }
      case '$options':
        return true;
      default:
        return false;
    }
  });
};

const matchesQuery = (doc, query = {}) => {
  if (!query || Object.keys(query).length === 0) {
    return true;
  }

  if (query.$or) {
    return query.$or.some(orCondition => matchesQuery(doc, orCondition));
  }

  return Object.entries(query).every(([key, condition]) => {
    const value = getValueByPath(doc, key);
    return matchesCondition(value, condition);
  });
};

const applyProjection = (doc, projection = {}) => {
  const includeKeys = Object.entries(projection)
    .filter(([, flag]) => flag)
    .map(([key]) => key);

  if (includeKeys.length === 0) {
    return cloneValue(doc);
  }

  const projected = {};
  includeKeys.forEach(key => {
    const value = getValueByPath(doc, key);
    if (value !== undefined) {
      setValueByPath(projected, key, cloneValue(value));
    }
  });

  return projected;
};

class MockCursor {
  constructor(documents) {
    this.documents = documents.map(cloneValue);
    this.sortSpec = null;
    this.limitValue = null;
    this.projection = null;
  }

  sort(sortSpec) {
    this.sortSpec = sortSpec;
    return this;
  }

  limit(limitValue) {
    this.limitValue = limitValue;
    return this;
  }

  project(projection) {
    this.projection = projection;
    return this;
  }

  async toArray() {
    let results = this.documents.map(cloneValue);

    if (this.sortSpec) {
      const sortEntries = Object.entries(this.sortSpec);
      results.sort((a, b) => {
        for (const [path, direction] of sortEntries) {
          const aValue = getValueByPath(a, path);
          const bValue = getValueByPath(b, path);

          if (aValue == null && bValue != null) {
            return direction > 0 ? -1 : 1;
          }
          if (aValue != null && bValue == null) {
            return direction > 0 ? 1 : -1;
          }
          if (aValue < bValue) {
            return direction > 0 ? -1 : 1;
          }
          if (aValue > bValue) {
            return direction > 0 ? 1 : -1;
          }
        }
        return 0;
      });
    }

    if (typeof this.limitValue === 'number') {
      results = results.slice(0, this.limitValue);
    }

    if (this.projection) {
      results = results.map(doc => applyProjection(doc, this.projection));
    }

    return results;
  }
}

class MockCollection {
  constructor(name) {
    this.name = name;
    this.documents = [];
  }

  clear() {
    this.documents = [];
  }

  _ensureObjectId(value) {
    if (value instanceof ObjectId) {
      return value;
    }
    return new ObjectId(value);
  }

  _findMatchingIndices(filter = {}) {
    return this.documents.reduce((indices, doc, index) => {
      if (matchesQuery(doc, filter)) {
        indices.push(index);
      }
      return indices;
    }, []);
  }

  async insertOne(doc) {
    const newDoc = cloneValue(doc);
    if (!newDoc._id) {
      newDoc._id = new ObjectId();
    } else {
      newDoc._id = this._ensureObjectId(newDoc._id);
    }
    this.documents.push(newDoc);
    return { acknowledged: true, insertedId: newDoc._id };
  }

  find(query = {}) {
    const matches = this.documents.filter(doc => matchesQuery(doc, query));
    return new MockCursor(matches);
  }

  async findOne(filter = {}, options = {}) {
    const match = this.documents.find(doc => matchesQuery(doc, filter));
    if (!match) {
      return null;
    }
    if (options.projection) {
      return applyProjection(match, options.projection);
    }
    return cloneValue(match);
  }

  async updateOne(filter, update = {}) {
    const indices = this._findMatchingIndices(filter);
    if (indices.length === 0) {
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0 };
    }

    const index = indices[0];
    const doc = cloneValue(this.documents[index]);
    if (update.$set) {
      Object.entries(update.$set).forEach(([path, value]) => {
        setValueByPath(doc, path, cloneValue(value));
      });
    }

    this.documents[index] = doc;
    return { acknowledged: true, matchedCount: 1, modifiedCount: update.$set ? 1 : 0 };
  }

  async updateMany(filter, update = {}) {
    const indices = this._findMatchingIndices(filter);
    if (indices.length === 0) {
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0 };
    }

    indices.forEach(index => {
      const doc = cloneValue(this.documents[index]);
      if (update.$set) {
        Object.entries(update.$set).forEach(([path, value]) => {
          setValueByPath(doc, path, cloneValue(value));
        });
      }
      this.documents[index] = doc;
    });

    return {
      acknowledged: true,
      matchedCount: indices.length,
      modifiedCount: update.$set ? indices.length : 0
    };
  }

  async deleteOne(filter = {}) {
    const index = this.documents.findIndex(doc => matchesQuery(doc, filter));
    if (index === -1) {
      return { acknowledged: true, deletedCount: 0 };
    }
    this.documents.splice(index, 1);
    return { acknowledged: true, deletedCount: 1 };
  }

  async deleteMany(filter = {}) {
    const beforeCount = this.documents.length;
    this.documents = this.documents.filter(doc => !matchesQuery(doc, filter));
    return { acknowledged: true, deletedCount: beforeCount - this.documents.length };
  }

  async countDocuments(filter = {}) {
    return this.documents.filter(doc => matchesQuery(doc, filter)).length;
  }
}

const createMockDatabaseConfig = () => {
  const collections = new Map();
  let connected = false;

  const resolveName = (collectionType = 'normalized') => {
    if (collectionType === 'normalized') {
      return 'asyncapi_normalized';
    }
    if (collectionType === 'original') {
      return 'asyncapi_originals';
    }
    return collectionType;
  };

  return {
    async connect() {
      connected = true;
      return {
        collection: name => {
          const resolved = resolveName(name);
          if (!collections.has(resolved)) {
            collections.set(resolved, new MockCollection(resolved));
          }
          return collections.get(resolved);
        }
      };
    },
    getCollection(collectionType = 'normalized') {
      if (!connected) {
        throw new Error('Database not connected');
      }
      const resolved = resolveName(collectionType);
      if (!collections.has(resolved)) {
        collections.set(resolved, new MockCollection(resolved));
      }
      return collections.get(resolved);
    },
    async close() {
      connected = false;
      collections.forEach(collection => collection.clear());
      collections.clear();
    },
    isDatabaseConnected() {
      return connected;
    }
  };
};

module.exports = {
  createMockDatabaseConfig
};
