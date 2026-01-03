const fs = require('fs-extra');
const { Parser } = require('@asyncapi/parser');
const { convert } = require('@asyncapi/converter');
const yaml = require('yaml');
const { buildAsyncService } = require('../../lib/async-service-builder');

// DB config (προσαρμόσε το path αν το αρχείο σου είναι αλλού)
const DatabaseConfig = require('../config/database');

// --- Small helpers ---------------------------------------------------------
const ensureArray = (v) => (Array.isArray(v) ? v : v == null ? [] : [v]);
const lc = (s) => (typeof s === 'string' ? s.toLowerCase() : '');

// --- Core class ------------------------------------------------------------
class AsyncAPIProcessor {
  constructor({ db } = {}) {
    this.supportedFormats = ['yaml', 'json', 'yml'];
    // δέχεται dependency injection για tests (mock) ή χρησιμοποιεί το κανονικό DB
    this.db = db || DatabaseConfig;

    // Backwards compatibility aliases (παλαιότερα scripts/cli calls)
    this.processAsyncAPIFile = this.processAsyncAPIFile.bind(this);
    this.processFile = this.process.bind(this);
  }

  /** Load file from disk as UTF-8 string */
  async load(filePath) {
    if (!(await fs.pathExists(filePath))) {
      throw new Error(`File not found: ${filePath}`);
    }
    const content = await fs.readFile(filePath, 'utf8');
    console.log(`📄 Loaded AsyncAPI file: ${filePath}`);
    return content;
  }

  /** Alias maintained for backwards compatibility */
  async loadAsyncAPIFile(filePath) {
    return this.load(filePath);
  }

  /** Parse AsyncAPI content (YAML or JSON) into plain JSON object */
  async parse(content) {
    try {
      const parser = new Parser();
      const { document, diagnostics } = await parser.parse(content);
      if (diagnostics?.length) {
        console.warn('⚠️ AsyncAPI diagnostics:', diagnostics.map((d) => d.message));
      }
      console.log('✅ AsyncAPI parsed successfully');
      return document.json();
    } catch (error) {
      console.error('❌ Error parsing AsyncAPI:', error.message);
      throw error;
    }
  }

  async parseAsyncAPI(content) {
    return this.parse(content);
  }

  /** Minimal validation for required info */
  validate(spec) {
    const errors = [];
    if (!spec.info) errors.push('Missing: info');
    if (!spec.info?.title) errors.push('Missing: info.title');
    if (!spec.info?.version) errors.push('Missing: info.version');

    const result = { isValid: errors.length === 0, errors, warnings: [] };
    console.log(`✅ Validation ${result.isValid ? 'passed' : 'failed'}`);
    return result;
  }

  validateAsyncAPI(asyncAPISpec) {
    const validation = this.validate(asyncAPISpec);

    if (!asyncAPISpec.channels || Object.keys(asyncAPISpec.channels).length === 0) {
      validation.warnings.push('No channels defined');
    }

    if (!asyncAPISpec.servers || Object.keys(asyncAPISpec.servers).length === 0) {
      validation.warnings.push('No servers defined');
    }

    return validation;
  }

  /** Create a flat metadata view from the AsyncAPI document (works for v2 & v3) */
  flattenMetadata(spec = {}) {
    const info = spec.info || {};
    const serversObj = spec.servers || {};
    const channelsObj = spec.channels || {};

    const getTags = () => {
      const raw = Array.isArray(spec.tags) ? spec.tags : (Array.isArray(info.tags) ? info.tags : []);
      return raw
        .map((t) => (typeof t === 'string' ? t : t?.name))
        .filter(Boolean);
    };

    // servers
    const servers = Object.entries(serversObj).map(([name, s]) => ({
      name,
      url: s?.url,
      protocol: s?.protocol,
      description: s?.description
    }));

    // channels
    const channels = Object.entries(channelsObj).map(([name, ch]) => ({
      name,
      description: ch?.description,
      // v3 μπορεί να έχει channel-level servers
      servers: Array.isArray(ch?.servers) ? ch.servers : undefined
    }));

    // operations (support both v3 `operations` και v2 publish/subscribe μέσα στα channels)
    const opsV3 = Object.entries(spec.operations || {}).map(([key, op]) => ({
      operationId: op?.operationId || key,
      action: op?.action,         // send | receive (v3)
      channel: op?.channel,
      summary: op?.summary
    }));

    const opsV2 = Object.entries(channelsObj).flatMap(([chName, ch]) => {
      const out = [];
      if (ch?.publish) {
        out.push({
          operationId: ch.publish.operationId,
          action: 'publish',
          channel: chName,
          summary: ch.publish.summary
        });
      }
      if (ch?.subscribe) {
        out.push({
          operationId: ch.subscribe.operationId,
          action: 'subscribe',
          channel: chName,
          summary: ch.subscribe.summary
        });
      }
      return out;
    });

    // messages (components.messages)
    const messagesObj = spec.components?.messages || {};
    const messages = Object.entries(messagesObj).map(([name, msg]) => ({
      name,
      messageId: msg?.messageId,
      title: msg?.title || msg?.name,
      summary: msg?.summary
    }));

    // protocols list (dedup)
    const protocols = Array.from(
      new Set(servers.map((s) => s.protocol).filter(Boolean))
    );

    return {
      id: spec.id || spec['x-id'] || '',
      title: info.title || 'Untitled API',
      version: info.version || '',
      description: info.description || '',
      tags: getTags(),
      defaultContentType: spec.defaultContentType,
      protocols,
      serversCount: servers.length,
      channelsCount: channels.length,
      servers,
      channels,
      operations: [...opsV3, ...opsV2],
      messages
    };
  }

  /** Convert to v3 (only if needed) and stringify to target format */
  async convert(originalContent, spec, targetFormat = 'json') {
    console.log('🔄 Starting conversion to target format');
    if (!['json', 'yaml'].includes(targetFormat)) {
      throw new Error(`Unsupported format: ${targetFormat}`);
    }

    const wantYaml = targetFormat === 'yaml';
    const isV3 = typeof spec?.asyncapi === 'string' && spec.asyncapi.startsWith('3.');

    let document = spec;
    let wasConverted = false;

    if (!isV3) {
      console.log('⚙️ Converting AsyncAPI spec to 3.0.0');
      const src = typeof originalContent === 'string' ? originalContent : JSON.stringify(originalContent, null, 2);
      const out = await convert(src, '3.0.0');

      if (typeof out === 'string') {
        document = out.trim().startsWith('{') ? JSON.parse(out) : yaml.parse(out);
      } else if (out?.document && typeof out.document === 'object') {
        document = out.document;
      } else if (out?.document && typeof out.document === 'string') {
        document = out.document.trim().startsWith('{') ? JSON.parse(out.document) : yaml.parse(out.document);
      } else if (out?.converted) {
        document = out.converted.trim().startsWith('{') ? JSON.parse(out.converted) : yaml.parse(out.converted);
      } else {
        document = out;
      }
      wasConverted = true;
    }

    const content = wantYaml ? yaml.stringify(document) : JSON.stringify(document, null, 2);
    const version = document?.asyncapi || '3.0.0';

    console.log(`✅ Conversion completed. Version: ${version}, Converted: ${wasConverted}`);
    return { content, document, version, wasConverted };
  }

  async convertAsyncAPI(originalContent, parsedSpec, targetFormat = 'json') {
    return this.convert(originalContent, parsedSpec, targetFormat);
  }

  /** Pass-through normalization (keeps raw spec structure) */
  normalize(spec) {
    console.log('🔧 Normalizing AsyncAPI data');
    return JSON.parse(JSON.stringify(spec));
  }

  normalizeAsyncAPIData(asyncAPISpec) {
    return this.normalize(asyncAPISpec);
  }

  /** Small, useful summary for lists/search */
  buildSummary(spec) {
    const info = spec.info || {};
    const servers = Object.values(spec.servers || {});
    const channels = Object.values(spec.channels || {});
    const protocols = Array.from(new Set(servers.map((s) => s.protocol).filter(Boolean)));

    const summary = {
      title: info.title || 'Untitled API',
      version: info.version || '',
      description: info.description || '',
      protocol: protocols[0] || 'unknown',
      protocols,
      channelsCount: channels.length,
      serversCount: servers.length,
      tags: ensureArray(spec.tags || info.tags).map((t) => (t?.name ?? t)).filter(Boolean),
      defaultContentType: spec.defaultContentType,
      createdAt: new Date(),
      updatedAt: new Date(),
      processedAt: new Date()
    };

    console.log('📊 Summary built successfully');
    return summary;
  }

  /** Lowercased fields used for text search */
  buildSearchableFields(summary = {}) {
    console.log('🔍 Building searchable fields');
    return {
      title: lc(summary.title),
      description: lc(summary.description),
      version: summary.version || '',
      protocol: lc(summary.protocol || ''),
      tags: ensureArray(summary.tags).map((t) => lc(String(t)))
    };
  }

  // ---------------------- NEW: originals & metada persistence ----------------------

  detectFormat(filePath, originalContent) {
    const lower = (filePath || '').toLowerCase();
    if (lower.endsWith('.json')) return 'json';
    if (lower.endsWith('.yaml') || lower.endsWith('.yml')) return 'yaml';
    const trimmed = (originalContent || '').trim();
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) return 'json';
    return 'yaml';
  }

  /**
   * Store the original file (raw string) into 'original' collection
   */
  async saveOriginal({ filePath, originalContent, normalizedId = null, extra = {} } = {}) {
    await this.db.connect();
    const originals = this.db.getCollection('original');

    const format = this.detectFormat(filePath, originalContent);
    const contentType = format === 'json' ? 'application/json' : 'text/yaml';

    const doc = {
      normalizedId: normalizedId || null,
      raw: originalContent,   // as-is
      format,
      contentType,
      filePath,
      createdAt: new Date(),
      ...extra
    };

    const { insertedId } = await originals.insertOne(doc);
    return insertedId;
  }

  /**
   * Store flattened metadata into 'metada' collection
   */
  async saveMetada({
    spec,
    filePath,
    originalId = null,
    normalizedId = null,
    flattened = null,
    asyncServiceDoc = null,
    extra = {}
  } = {}) {
    if (!spec) throw new Error('Missing spec for metada insert');
    await this.db.connect();
    const metada = this.db.getCollection('metada');

    const flat = flattened || this.flattenMetadata(spec);
    const protocols = Array.isArray(flat?.protocols) ? flat.protocols : [];
    const primaryProtocol = protocols.find(Boolean);

    const asyncServiceData = asyncServiceDoc || buildAsyncService(spec);
    const asyncServiceArray = Array.isArray(asyncServiceData?.AsyncService)
      ? asyncServiceData.AsyncService
      : [];
    const asyncServiceEntry = asyncServiceArray[0] || null;
    const now = new Date();

    const doc = {
      ...flat,
      protocol: flat?.protocol || primaryProtocol,
      protocols,
      AsyncService: asyncServiceArray,
      asyncService: asyncServiceEntry,
      asyncServiceId: asyncServiceEntry?.id,
      originalId: originalId || null,
      normalizedId: normalizedId || null,
      filePath
    };

    if (doc.protocol == null && Array.isArray(asyncServiceEntry?.Server)) {
      doc.protocol = asyncServiceEntry.Server.map((server) => server?.protocol).find(Boolean);
    }

    const dedupeFilter = asyncServiceEntry?.id
      ? { asyncServiceId: asyncServiceEntry.id }
      : { title: flat.title, version: flat.version, filePath };

    const result = await metada.findOneAndUpdate(
      dedupeFilter,
      {
        $set: {
          ...doc,
          updatedAt: now
        },
        $setOnInsert: {
          createdAt: now
        }
      },
      { upsert: true, returnDocument: 'after' }
    );

    return result?.value?._id || result?.lastErrorObject?.upserted;
  }

  /** End-to-end helper: load → parse → validate → convert → normalize → summarize → persist originals & metada */
  async process(filePath, targetFormat = 'json', options = {}) {
    try {
      console.log(`🚀 Starting AsyncAPI processing for: ${filePath}`);
      const original = await this.load(filePath);

      const { persistOriginal = true, persistMetada = true } = options;
      let originalId = null;
      let metadaId = null;

      // 1) Save original as-is
      if (persistOriginal) {
        originalId = await this.saveOriginal({ filePath, originalContent: original });
        console.log(`🗄️ Stored original with _id: ${originalId}`);
      }

      // 2) Parse/validate
      const parsed = await this.parse(original);
      const validation = this.validateAsyncAPI(parsed);
      if (!validation.isValid) {
        throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
      }

      // 3) Convert (to v3 if needed) and normalize/summary/searchFields/flatten
      const conversion = await this.convert(original, parsed, targetFormat);
      const normalized = this.normalize(conversion.document);
      const summary = this.buildSummary(conversion.document);
      const searchableFields = this.buildSearchableFields(summary);
      const flattened = this.flattenMetadata(conversion.document);
      const asyncService = buildAsyncService(conversion.document);

      // 4) Save metada
      if (persistMetada) {
        metadaId = await this.saveMetada({
          spec: conversion.document,
          filePath,
          originalId,
          flattened,
          asyncServiceDoc: asyncService
          // normalizedId: αν αργότερα δημιουργείς doc στη normalized συλλογή, πέρασέ το εδώ
        });
        console.log(`🧾 Stored metada with _id: ${metadaId}`);
      }

      console.log('✅ AsyncAPI processing completed successfully');
      return {
        filePath,
        original,
        parsed,
        converted: conversion.content,
        normalized,
        summary,
        searchableFields,
        flattened,
        asyncService,
        validation,
        originalId,
        metadaId
      };
    } catch (error) {
      console.error('❌ AsyncAPI processing failed:', error.message);
      throw error;
    }
  }

  async processAsyncAPIFile(filePath, targetFormat = 'json') {
    return this.process(filePath, targetFormat);
  }

  async processAsyncAPI(filePath, targetFormat = 'json') {
    return this.processAsyncAPIFile(filePath, targetFormat);
  }
}

module.exports = AsyncAPIProcessor;
module.exports.AsyncAPIProcessor = AsyncAPIProcessor;
module.exports.default = AsyncAPIProcessor;
