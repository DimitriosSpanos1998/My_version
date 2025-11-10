const fs = require('fs-extra');
const { Parser } = require('@asyncapi/parser');
const { convert } = require('@asyncapi/converter');
const yaml = require('yaml');

// --- Small helpers ---------------------------------------------------------
const ensureArray = (v) => (Array.isArray(v) ? v : v == null ? [] : [v]);
const lc = (s) => (typeof s === 'string' ? s.toLowerCase() : '');

// --- Core class ------------------------------------------------------------
class AsyncAPIProcessor {
  constructor() {
    this.supportedFormats = ['yaml', 'json', 'yml'];
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

  /** End-to-end helper: load → parse → validate → convert → normalize → summarize */
  async process(filePath, targetFormat = 'json') {
    try {
      console.log(`🚀 Starting AsyncAPI processing for: ${filePath}`);
      const original = await this.load(filePath);
      const parsed = await this.parse(original);
      const validation = this.validateAsyncAPI(parsed);
      if (!validation.isValid) {
        throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
      }

      const conversion = await this.convert(original, parsed, targetFormat);
      const normalized = this.normalize(conversion.document);
      const summary = this.buildSummary(conversion.document);
      const searchableFields = this.buildSearchableFields(summary);

      console.log('✅ AsyncAPI processing completed successfully');
      return { original, parsed, converted: conversion.content, normalized, summary, searchableFields, validation };
    } catch (error) {
      console.error('❌ AsyncAPI processing failed:', error.message);
      throw error;
    }
  }

  async processAsyncAPIFile(filePath, targetFormat = 'json') {
    return this.process(filePath, targetFormat);
  }
}

module.exports = AsyncAPIProcessor;
