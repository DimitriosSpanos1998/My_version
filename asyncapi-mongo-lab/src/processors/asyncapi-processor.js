const fs = require('fs-extra');
const { convert } = require('@asyncapi/converter');
const yaml = require('yaml');
const { randomUUID } = require('crypto');

// --- Small helpers ---------------------------------------------------------
const ensureArray = (value) => (Array.isArray(value) ? value : value == null ? [] : [value]);
const lc = (value) => (typeof value === 'string' ? value.toLowerCase() : '');
const tagNames = (tags) => ensureArray(tags).map((tag) => tag?.name ?? tag).filter(Boolean);

const parseContent = (content) => {
  if (content == null) {
    throw new Error('No AsyncAPI content provided');
  }

  if (typeof content !== 'string') {
    return JSON.parse(JSON.stringify(content));
  }

  try {
    return JSON.parse(content);
  } catch (jsonError) {
    try {
      return yaml.parse(content);
    } catch (yamlError) {
      const parsingError = new Error('Unable to parse AsyncAPI content');
      parsingError.cause = yamlError;
      throw parsingError;
    }
  }
};

const extractOperations = (asyncapi, channel) => {
  const operations = [];

  if (Array.isArray(channel?.operations)) {
    operations.push(...channel.operations);
  } else if (channel?.operations && typeof channel.operations === 'object') {
    for (const [action, operation] of Object.entries(channel.operations)) {
      operations.push({ action, ...(operation || {}) });
    }
  }

  ['publish', 'subscribe'].forEach((action) => {
    if (channel?.[action]) {
      operations.push({ action, ...channel[action] });
    }
  });

  return operations.map((operation) => {
    const action = operation.action ?? operation.type ?? 'publish';
    const rawMessages = Array.isArray(operation.messages)
      ? operation.messages
      : operation.message != null
        ? ensureArray(operation.message)
        : [];

    const messages = rawMessages.map((message) => ({
      name: message?.name,
      title: message?.title,
      summary: message?.summary,
      contentType: message?.contentType ?? asyncapi?.defaultContentType,
      schemaFormat: message?.schemaFormat,
      correlationId: message?.correlationId?.location ?? message?.correlationId,
      bindings: message?.bindings ?? {},
      examples: message?.examples ?? [],
      payloadSchema: message?.payload,
      headersSchema: message?.headers
    }));

    return {
      action,
      operationId: operation?.operationId,
      summary: operation?.summary,
      description: operation?.description,
      tags: tagNames(operation?.tags),
      security: ensureArray(operation?.security).map((item) =>
        typeof item === 'object' && item !== null ? Object.keys(item)[0] : item
      ),
      bindings: operation?.bindings ?? {},
      messages
    };
  });
};

const buildFlattenedMetadata = (asyncapi, serviceId) => {
  const info = asyncapi?.info ?? {};

  const service = {
    id: serviceId ?? info?.id ?? randomUUID(),
    title: info?.title,
    version: info?.version,
    defaultContentType: asyncapi?.defaultContentType,
    description: info?.description,
    tags: tagNames(asyncapi?.tags ?? info?.tags)
  };

  const servers = Object.entries(asyncapi?.servers ?? {}).map(([name, server]) => ({
    name,
    url: server?.url,
    protocol: server?.protocol,
    protocolVersion: server?.protocolVersion,
    description: server?.description,
    security: ensureArray(server?.security).map((item) =>
      typeof item === 'object' && item !== null ? Object.keys(item)[0] : item
    ),
    bindings: server?.bindings ?? {},
    variables: Object.entries(server?.variables ?? {}).map(([variableName, variable]) => ({
      name: variableName,
      default: variable?.default,
      enum: variable?.enum ?? [],
      description: variable?.description
    }))
  }));

  const channels = Object.entries(asyncapi?.channels ?? {}).map(([name, channel]) => ({
    name,
    description: channel?.description,
    parameters: Object.keys(channel?.parameters ?? {}),
    bindings: channel?.bindings ?? {},
    operations: extractOperations(asyncapi, channel)
  }));

  const securities = Object.entries(asyncapi?.components?.securitySchemes ?? {}).map(
    ([securityName, scheme]) => {
      const security = {
        name: securityName,
        type: scheme?.type
      };

      ['in', 'scheme', 'bearerFormat', 'openIdConnectUrl'].forEach((key) => {
        if (scheme?.[key]) {
          security[key] = scheme[key];
        }
      });

      if (scheme?.type === 'oauth2') {
        security.flows = Object.fromEntries(
          Object.entries(scheme?.flows ?? {}).map(([flowName, flow]) => [
            flowName,
            {
              authorizationUrl: flow?.authorizationUrl,
              tokenUrl: flow?.tokenUrl,
              refreshUrl: flow?.refreshUrl,
              scopes: Object.keys(flow?.scopes ?? {})
            }
          ])
        );
      }

      return security;
    }
  );

  return { service, servers, channels, securities };
};

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
      const parsed = parseContent(content);
      console.log('✅ AsyncAPI parsed successfully');
      return parsed;
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
    const document = parseContent(spec);
    const currentVersion = typeof document?.asyncapi === 'string' ? document.asyncapi : document?.version;

    let convertedDocument = document;
    let wasConverted = false;

    if (typeof currentVersion !== 'string' || !currentVersion.startsWith('3.')) {
      console.log('⚙️ Converting AsyncAPI spec to 3.0.0');
      const source =
        typeof originalContent === 'string'
          ? originalContent
          : yaml.stringify(originalContent ?? document);

      const output = await convert(source, '3.0.0');
      let convertedContent = null;

      if (typeof output === 'string') {
        convertedContent = output;
      } else if (output?.converted) {
        convertedContent = output.converted;
      } else if (typeof output?.document === 'string') {
        convertedContent = output.document;
      } else if (output?.document && typeof output.document === 'object') {
        convertedDocument = output.document;
      } else if (output) {
        convertedDocument = output;
      }

      if (convertedContent != null) {
        convertedDocument = convertedContent.trim().startsWith('{')
          ? JSON.parse(convertedContent)
          : yaml.parse(convertedContent);
      }

      wasConverted = true;
    }

    const content = wantYaml
      ? yaml.stringify(convertedDocument)
      : JSON.stringify(convertedDocument, null, 2);
    const version = convertedDocument?.asyncapi || '3.0.0';

    console.log(`✅ Conversion completed. Version: ${version}, Converted: ${wasConverted}`);
    return { content, document: convertedDocument, version, wasConverted };
  }

  async convertAsyncAPI(originalContent, parsedSpec, targetFormat = 'json') {
    return this.convert(originalContent, parsedSpec, targetFormat);
  }

  /** Normalize AsyncAPI document into flattened metadata */
  normalize(spec, options = {}) {
    console.log('🔧 Normalizing AsyncAPI data');
    const document = parseContent(spec);
    const metadata = buildFlattenedMetadata(document, options?.serviceId);

    return {
      asyncapi: document?.asyncapi ?? '3.0.0',
      info: document?.info ?? {},
      defaultContentType: document?.defaultContentType,
      service: metadata.service,
      servers: metadata.servers,
      channels: metadata.channels,
      securities: metadata.securities,
      components: document?.components ?? {},
      tags: metadata.service.tags
    };
  }

  normalizeAsyncAPIData(asyncAPISpec, options = {}) {
    return this.normalize(asyncAPISpec, options);
  }

  flattenMetadata(spec, options = {}) {
    return buildFlattenedMetadata(parseContent(spec), options?.serviceId);
  }

  /** Small, useful summary for lists/search */
  buildSummary(normalized = {}) {
    const info = normalized.info || {};
    const service = normalized.service || {};
    const servers = Array.isArray(normalized.servers)
      ? normalized.servers
      : Object.values(normalized.servers || {});
    const channels = Array.isArray(normalized.channels)
      ? normalized.channels
      : Object.values(normalized.channels || {});
    const protocols = Array.from(new Set(servers.map((server) => server?.protocol).filter(Boolean)));

    const summary = {
      title: service.title || info.title || 'Untitled API',
      version: service.version || info.version || '',
      description: service.description || info.description || '',
      protocol: protocols[0] || 'unknown',
      protocols,
      channelsCount: channels.length,
      serversCount: servers.length,
      tags: tagNames(service.tags?.length ? service.tags : normalized.tags ?? info.tags),
      defaultContentType: service.defaultContentType ?? normalized.defaultContentType,
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
      tags: ensureArray(summary.tags).map((tag) => lc(String(tag)))
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
      const summary = this.buildSummary(normalized);
      const searchableFields = this.buildSearchableFields(summary);
      const flattened = this.flattenMetadata(conversion.document);

      console.log('✅ AsyncAPI processing completed successfully');
      return {
        original,
        parsed,
        converted: conversion.content,
        normalized,
        summary,
        searchableFields,
        validation,
        flattened
      };
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
