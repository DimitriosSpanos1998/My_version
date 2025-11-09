const fs = require('fs-extra');
const { Parser } = require('@asyncapi/parser');
const { convert } = require('@asyncapi/converter');
const yaml = require('yaml');
const { randomUUID } = require('node:crypto');

function ensureArray(value) {
  if (Array.isArray(value)) return value;
  if (value == null) return [];
  return [value];
}

function tagNames(tags) {
  return ensureArray(tags)
    .map(tag => tag?.name ?? tag)
    .filter(Boolean);
}

function extractOperations(asyncapi, channel) {
  const operations = [];

  if (Array.isArray(channel?.operations)) {
    for (const op of channel.operations) operations.push(op);
  } else if (channel?.operations && typeof channel.operations === 'object') {
    for (const [action, op] of Object.entries(channel.operations)) {
      operations.push({ action, ...op });
    }
  }

  ['publish', 'subscribe'].forEach(action => {
    if (channel?.[action]) operations.push({ action, ...channel[action] });
  });

  return operations.map(op => {
    const action = op.action ?? op.type ?? 'publish';
    const rawMessages = Array.isArray(op.messages)
      ? op.messages
      : op.message != null
        ? Array.isArray(op.message)
          ? op.message
          : [op.message]
        : [];

    const messages = rawMessages.map(message => ({
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
      operationId: op?.operationId,
      summary: op?.summary,
      description: op?.description,
      tags: tagNames(op?.tags),
      security: ensureArray(op?.security).map(entry =>
        typeof entry === 'object' ? Object.keys(entry)[0] : entry
      ),
      bindings: op?.bindings ?? {},
      messages
    };
  });
}

function buildMetadataStructure(asyncapi, serviceId) {
  const info = asyncapi?.info ?? {};

  const service = {
    id: serviceId ?? randomUUID(),
    title: info.title,
    version: info.version,
    defaultContentType: asyncapi.defaultContentType,
    description: info.description,
    tags: tagNames(asyncapi.tags ?? info.tags)
  };

  const servers = Object.entries(asyncapi.servers ?? {}).map(([name, server]) => ({
    name,
    url: server.url,
    protocol: server.protocol,
    protocolVersion: server.protocolVersion,
    description: server.description,
    security: ensureArray(server.security).map(entry =>
      typeof entry === 'object' ? Object.keys(entry)[0] : entry
    ),
    bindings: server.bindings ?? {},
    variables: Object.entries(server.variables ?? {}).map(([varName, variable]) => ({
      name: varName,
      default: variable?.default,
      enum: variable?.enum ?? [],
      description: variable?.description
    }))
  }));

  const channels = Object.entries(asyncapi.channels ?? {}).map(([name, channel]) => ({
    name,
    description: channel?.description,
    parameters: Object.keys(channel?.parameters ?? {}),
    bindings: channel?.bindings ?? {},
    operations: extractOperations(asyncapi, channel)
  }));

  const securities = Object.entries(asyncapi?.components?.securitySchemes ?? {}).map(([name, scheme]) => {
    const entry = { name, type: scheme.type };

    ['in', 'scheme', 'bearerFormat', 'openIdConnectUrl'].forEach(key => {
      if (scheme[key]) {
        entry[key] = scheme[key];
      }
    });

    if (scheme.type === 'oauth2') {
      entry.flows = Object.fromEntries(
        Object.entries(scheme.flows ?? {}).map(([flowName, flow]) => [
          flowName,
          {
            authorizationUrl: flow.authorizationUrl,
            tokenUrl: flow.tokenUrl,
            refreshUrl: flow.refreshUrl,
            scopes: Object.keys(flow.scopes ?? {})
          }
        ])
      );
    }

    return entry;
  });

  return { service, servers, channels, securities };
}

class AsyncAPIProcessor {
  constructor() {
    this.supportedFormats = ['yaml', 'json', 'yml'];
  }

  /**
   * Load AsyncAPI file from filesystem
   * @param {string} filePath - Path to the AsyncAPI file
   * @returns {Promise<string>} File content as string
   */
  async loadAsyncAPIFile(filePath) {
    try {
      if (!await fs.pathExists(filePath)) {
        throw new Error(`File not found: ${filePath}`);
      }

      const content = await fs.readFile(filePath, 'utf8');
      console.log(`📄 Loaded AsyncAPI file: ${filePath}`);
      return content;
    } catch (error) {
      console.error(`❌ Error loading file ${filePath}:`, error.message);
      throw error;
    }
  }

  /**
   * Parse AsyncAPI specification
   * @param {string} content - AsyncAPI content as string
   * @returns {Promise<Object>} Parsed AsyncAPI object
   */
  async parseAsyncAPI(content, filePath) {
    try {
      // First, parse YAML to get the raw object
      let parsedYAML;
      if (filePath.endsWith('.yaml') || filePath.endsWith('.yml')) {
        parsedYAML = yaml.parse(content);
      } else {
        parsedYAML = JSON.parse(content);
      }

      // Now use the AsyncAPI parser to validate (optional but good practice)
      const parser = new Parser();
      await parser.parse(parsedYAML);
      
      console.log('✅ AsyncAPI specification parsed successfully');
      
      // Return the parsed YAML/JSON as plain object
      return parsedYAML;
    } catch (error) {
      console.error('❌ Error parsing AsyncAPI:', error.message);
      throw error;
    }
  }

  /**
   * Convert AsyncAPI to different format
   Basically, 'convert' means stringifying the JSON or converting to YAML
   * @param {Object} asyncAPISpec - Parsed AsyncAPI object (already JSON)
   * @param {string} targetFormat - Target format ('json', 'yaml')
   * @returns {string} Converted AsyncAPI as string
   */
  async convertAsyncAPI(originalContent, parsedSpec, targetFormat = 'json') {
    try {
      if (!['json', 'yaml'].includes(targetFormat)) {
        throw new Error(`Unsupported format: ${targetFormat}`);
      }

      const targetVersion = '3.0.0';
      const currentVersion = parsedSpec?.asyncapi || parsedSpec?.version;
      let convertedObject = parsedSpec;
      let wasConverted = false;

      if (typeof currentVersion !== 'string' || !currentVersion.startsWith('3.')) {
        const convertedDocument = await convert(originalContent, targetVersion);
        convertedObject = yaml.parse(convertedDocument);
        wasConverted = true;
      }

      const stringified =
        targetFormat === 'yaml'
          ? yaml.stringify(convertedObject)
          : JSON.stringify(convertedObject, null, 2);

      const resultingVersion = convertedObject?.asyncapi || convertedObject?.version;

      console.log(
        `🔄 Converted AsyncAPI to version ${resultingVersion || targetVersion} (${targetFormat.toUpperCase()})`
      );

      return {
        content: stringified,
        document: convertedObject,
        version: resultingVersion || targetVersion,
        wasConverted
      };
    } catch (error) {
      console.error('❌ Error converting AsyncAPI specification:', error.message);
      throw error;
    }
  }

  /**
   * Normalize AsyncAPI data for MongoDB storage
   * @param {Object} asyncAPISpec - Parsed AsyncAPI object
   * @returns {Object} Normalized data with metadata
   */
  normalizeAsyncAPIData(asyncAPISpec) {
    try {
      const normalized = JSON.parse(JSON.stringify(asyncAPISpec));

      console.log('🔧 AsyncAPI data normalized (raw JSON preserved)');
      return normalized;
    } catch (error) {
      console.error('❌ Error normalizing AsyncAPI data:', error.message);
      throw error;
    }
  }

  buildMetadata(asyncAPISpec) {
    try {
      const structure = buildMetadataStructure(asyncAPISpec);
      const now = new Date();

      const primaryProtocol =
        structure.servers.find(server => server.protocol)?.protocol || 'unknown';

      const metadata = {
        ...structure,
        title: structure.service.title || 'Untitled API',
        version: structure.service.version || '1.0.0',
        description: structure.service.description || '',
        protocol: primaryProtocol || 'unknown',
        protocols: structure.servers.map(server => server.protocol).filter(Boolean),
        channelsCount: structure.channels.length,
        serversCount: structure.servers.length,
        tags: structure.service.tags || [],
        defaultContentType: structure.service.defaultContentType,
        createdAt: now,
        updatedAt: now,
        processedAt: now
      };

      return metadata;
    } catch (error) {
      console.error('❌ Error building metadata:', error.message);
      throw error;
    }
  }

  buildSearchableFields(metadata = {}) {
    const service = metadata.service || {};
    const tags = metadata.tags || service.tags || [];

    const protocol = (metadata.protocol || '').toString().toLowerCase();

    return {
      title: (metadata.title || service.title || '').toString().toLowerCase(),
      description: (metadata.description || service.description || '').toString().toLowerCase(),
      version: metadata.version || service.version || '',
      protocol,
      tags: ensureArray(tags)
        .map(tag => tag.toString().toLowerCase())
        .filter(Boolean)
    };
  }

  /**
   * Validate AsyncAPI specification
   * @param {Object} asyncAPISpec - Parsed AsyncAPI object
   * @returns {Object} Validation result
   */
  validateAsyncAPI(asyncAPISpec) {
    const validation = {
      isValid: true,
      errors: [],
      warnings: []
    };

    // Check required fields
    if (!asyncAPISpec.info) {
      validation.errors.push('Missing required field: info');
      validation.isValid = false;
    }

    if (!asyncAPISpec.info?.title) {
      validation.errors.push('Missing required field: info.title');
      validation.isValid = false;
    }

    if (!asyncAPISpec.info?.version) {
      validation.errors.push('Missing required field: info.version');
      validation.isValid = false;
    }

    // Check for channels
    if (!asyncAPISpec.channels || Object.keys(asyncAPISpec.channels).length === 0) {
      validation.warnings.push('No channels defined');
    }

    // Check for servers
    if (!asyncAPISpec.servers || Object.keys(asyncAPISpec.servers).length === 0) {
      validation.warnings.push('No servers defined');
    }

    console.log(`✅ AsyncAPI validation completed. Valid: ${validation.isValid}`);
    return validation;
  }

  /**
   * Process AsyncAPI file end-to-end
   * @param {string} filePath - Path to AsyncAPI file
   * @param {string} targetFormat - Target format for conversion
   * @returns {Promise<Object>} Processed AsyncAPI data
   */
  async processAsyncAPIFile(filePath, targetFormat = 'json') {
    try {
      console.log(`🚀 Starting AsyncAPI processing for: ${filePath}`);
      
      // Load file
      const content = await this.loadAsyncAPIFile(filePath);
      
      // Parse AsyncAPI
      const parsed = await this.parseAsyncAPI(content, filePath);
      
      // Validate
      const validation = this.validateAsyncAPI(parsed);
      if (!validation.isValid) {
        throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
      }
      
      // Convert format
      const conversion = await this.convertAsyncAPI(content, parsed, targetFormat);

      // Normalize for MongoDB
      const normalized = this.normalizeAsyncAPIData(conversion.document);

      const metadata = this.buildMetadata(conversion.document);
      const searchableFields = this.buildSearchableFields(metadata);

      console.log('✅ AsyncAPI processing completed successfully');

      return {
        original: content,
        parsed: parsed,
        converted: conversion.content,
        normalized: normalized,
        metadata,
        searchableFields,
        validation: validation
      };
    } catch (error) {
      console.error('❌ AsyncAPI processing failed:', error.message);
      throw error;
    }
  }
}

module.exports = AsyncAPIProcessor;
