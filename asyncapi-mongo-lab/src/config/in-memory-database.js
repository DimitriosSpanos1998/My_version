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
    const cloned = {};
    for (const [key, val] of Object.entries(value)) {
      cloned[key] = cloneValue(val);
    }
    return cloned;
  }
  return value;
};

const getValueByPath = (obj, path) => {
  if (!path) return undefined;
  const parts = path.split('.');
  let current = obj;
  for (const part of parts) {
    if (current == null) {
      return undefined;
    }
    current = current[part];
  }
  return current;
};

const setValueByPath = (obj, path, value) => {
  const parts = path.split('.');
  let current = obj;
  parts.forEach((part, index) => {
    if (index === parts.length - 1) {
      current[part] = value;
    } else {
      if (!current[part] || typeof current[part] !== 'object') {
        current[part] = {};
      }
      current = current[part];
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
  if (condition && typeof condition === 'object' && !(condition instanceof Date) && !(condition instanceof ObjectId) && !Array.isArray(condition)) {
    for (const [operator, expected] of Object.entries(condition)) {
      switch (operator) {
        case '$gt':
          if (!(value > expected)) return false;
          break;
        case '$gte':
          if (!(value >= expected)) return false;
          break;
        case '$lt':
          if (!(value < expected)) return false;
          break;
        case '$lte':
          if (!(value <= expected)) return false;
          break;
        case '$in': {
          const list = expected || [];
          if (Array.isArray(value)) {
            const found = value.some(item => list.some(target => equals(item, target)));
            if (!found) return false;
          } else {
            const found = list.some(item => equals(value, item));
            if (!found) return false;
          }
          break;
        }
        case '$regex': {
          if (value === undefined || value === null) return false;
          const flags = condition.$options || '';
          const pattern = expected instanceof RegExp ? expected : new RegExp(expected, flags);
          if (Array.isArray(value)) {
            const matches = value.some(item => pattern.test(String(item)));
            if (!matches) return false;
          } else if (!pattern.test(String(value))) {
            return false;
          }
          break;
        }
        case '$options':
          // Handled alongside $regex
          break;
        default:
          return false;
      }
    }
    return true;
  }
  return equals(value, condition);
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

class InMemoryCursor {
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
      const sortKeys = Object.entries(this.sortSpec);
      results.sort((a, b) => {
        for (const [key, direction] of sortKeys) {
          const aValue = getValueByPath(a, key);
          const bValue = getValueByPath(b, key);
          if (aValue < bValue) return -1 * direction;
          if (aValue > bValue) return 1 * direction;
        }
        return 0;
      });
    }

    if (typeof this.limitValue === 'number') {
      results = results.slice(0, this.limitValue);
    }

    if (this.projection) {
      const includeKeys = Object.entries(this.projection)
        .filter(([, value]) => value)
        .map(([key]) => key);
      results = results.map(doc => {
        const projected = {};
        includeKeys.forEach(key => {
          const value = getValueByPath(doc, key);
          if (value !== undefined) {
            setValueByPath(projected, key, cloneValue(value));
          }
        });
        if (includeKeys.length === 0) {
          return doc;
        }
        return projected;
      });
    }

    return results;
  }
}

class InMemoryCollection {
  constructor(name) {
    this.name = name;
    this.documents = [];
  }

  async insertOne(document) {
    const doc = cloneValue(document);
    if (!doc._id) {
      doc._id = new ObjectId();
    }
    this.documents.push(doc);
    return { insertedId: doc._id };
  }

  find(query = {}) {
    const matches = this.documents.filter(doc => matchesQuery(doc, query));
    return new InMemoryCursor(matches);
  }

  async findOne(query = {}) {
    const match = this.documents.find(doc => matchesQuery(doc, query));
    return match ? cloneValue(match) : null;
  }

  async updateOne(filter, update) {
    const document = this.documents.find(doc => matchesQuery(doc, filter));
    if (!document) {
      return { matchedCount: 0, modifiedCount: 0 };
    }

    if (update && update.$set) {
      Object.entries(update.$set).forEach(([key, value]) => {
        setValueByPath(document, key, cloneValue(value));
      });
    }

    return { matchedCount: 1, modifiedCount: 1 };
  }

  async updateMany(filter, update) {
    let modifiedCount = 0;
    for (const document of this.documents) {
      if (matchesQuery(document, filter)) {
        if (update && update.$set) {
          Object.entries(update.$set).forEach(([key, value]) => {
            setValueByPath(document, key, cloneValue(value));
          });
        }
        modifiedCount += 1;
      }
    }
    return { matchedCount: modifiedCount, modifiedCount };
  }

  async deleteOne(filter) {
    const index = this.documents.findIndex(doc => matchesQuery(doc, filter));
    if (index === -1) {
      return { deletedCount: 0 };
    }
    this.documents.splice(index, 1);
    return { deletedCount: 1 };
  }

  async deleteMany(filter) {
    if (!filter || Object.keys(filter).length === 0) {
      const deletedCount = this.documents.length;
      this.documents = [];
      return { deletedCount };
    }

    const remaining = [];
    let deletedCount = 0;
    for (const doc of this.documents) {
      if (matchesQuery(doc, filter)) {
        deletedCount += 1;
      } else {
        remaining.push(doc);
      }
    }
    this.documents = remaining;
    return { deletedCount };
  }

  async countDocuments(filter = {}) {
    return this.documents.filter(doc => matchesQuery(doc, filter)).length;
  }

  aggregate(pipeline = []) {
    let results = this.documents.map(cloneValue);

    for (const stage of pipeline) {
      const [operator, spec] = Object.entries(stage)[0];
      switch (operator) {
        case '$match':
          results = results.filter(doc => matchesQuery(doc, spec));
          break;
        case '$group':
          results = aggregateGroup(results, spec);
          break;
        case '$sort':
          results = results.slice();
          const sortEntries = Object.entries(spec);
          results.sort((a, b) => {
            for (const [key, direction] of sortEntries) {
              const aValue = getValueByPath(a, key);
              const bValue = getValueByPath(b, key);
              if (aValue < bValue) return -1 * direction;
              if (aValue > bValue) return 1 * direction;
            }
            return 0;
          });
          break;
        case '$addFields':
          results = results.map(doc => applyAddFields(doc, spec));
          break;
        default:
          break;
      }
    }

    return {
      async toArray() {
        return results.map(cloneValue);
      }
    };
  }

  async createIndex() {
    return { created: true };
  }

  async dropIndexes() {
    return { dropped: true };
  }
}

const resolveExpressionValue = (doc, expression) => {
  if (typeof expression === 'string' && expression.startsWith('$')) {
    return getValueByPath(doc, expression.slice(1));
  }
  return expression;
};

const aggregateGroup = (docs, spec) => {
  const groups = new Map();
  const fieldSpecs = Object.entries(spec).filter(([key]) => key !== '_id');

  const getGroupState = key => {
    const stringKey = key instanceof ObjectId ? key.toHexString() : key;
    if (!groups.has(stringKey)) {
      groups.set(stringKey, {
        _id: key,
        __meta: fieldSpecs.reduce((acc, [field, definition]) => {
          if ('$avg' in definition) {
            acc[field] = { sum: 0, count: 0 };
          } else if ('$addToSet' in definition) {
            acc[field] = new Set();
          } else {
            acc[field] = null;
          }
          return acc;
        }, {})
      });
    }
    return groups.get(stringKey);
  };

  docs.forEach(doc => {
    const groupKey = resolveExpressionValue(doc, spec._id);
    const group = getGroupState(groupKey);

    fieldSpecs.forEach(([field, definition]) => {
      if ('$sum' in definition) {
        const value = definition.$sum === 1 ? 1 : resolveExpressionValue(doc, definition.$sum) || 0;
        group[field] = (group[field] || 0) + value;
      } else if ('$avg' in definition) {
        const value = resolveExpressionValue(doc, definition.$avg) || 0;
        group.__meta[field].sum += value;
        group.__meta[field].count += 1;
      } else if ('$push' in definition) {
        const value = resolveExpressionValue(doc, definition.$push);
        if (!group[field]) {
          group[field] = [];
        }
        group[field].push(value);
      } else if ('$addToSet' in definition) {
        const value = resolveExpressionValue(doc, definition.$addToSet);
        group.__meta[field].add(value);
      }
    });
  });

  const results = [];
  groups.forEach(group => {
    fieldSpecs.forEach(([field, definition]) => {
      if ('$avg' in definition) {
        const meta = group.__meta[field];
        group[field] = meta.count === 0 ? 0 : meta.sum / meta.count;
      } else if ('$addToSet' in definition) {
        group[field] = Array.from(group.__meta[field]);
      }
    });
    delete group.__meta;
    results.push(group);
  });

  return results;
};

const applyAddFields = (doc, spec) => {
  const updated = cloneValue(doc);
  Object.entries(spec).forEach(([field, expression]) => {
    if (expression && typeof expression === 'object' && '$divide' in expression) {
      const [numeratorExpr, denominatorExpr] = expression.$divide;
      const numerator = resolveExpressionValue(updated, numeratorExpr) || 0;
      const denominator = resolveExpressionValue(updated, denominatorExpr) || 1;
      setValueByPath(updated, field, denominator === 0 ? 0 : numerator / denominator);
    } else {
      const value = resolveExpressionValue(updated, expression);
      setValueByPath(updated, field, value);
    }
  });
  return updated;
};

class InMemoryDatabase {
  constructor() {
    this.collections = new Map();
  }

  collection(name) {
    return this.getCollection(name);
  }

  getCollection(name) {
    if (!this.collections.has(name)) {
      this.collections.set(name, new InMemoryCollection(name));
    }
    return this.collections.get(name);
  }

  close() {
    this.collections.clear();
  }
}

module.exports = InMemoryDatabase;
