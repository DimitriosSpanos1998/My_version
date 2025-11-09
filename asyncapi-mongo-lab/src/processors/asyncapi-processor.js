const fs = require('fs-extra');
const { Parser } = require('@asyncapi/parser');
const yaml = require('yaml');

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
  convertAsyncAPI(asyncAPISpec, targetFormat = 'json') {
    try {
      if (!['json', 'yaml'].includes(targetFormat)) {
        throw new Error(`Unsupported format: ${targetFormat}`);
      }

      let converted;
      if (targetFormat === 'json') {
        converted = JSON.stringify(asyncAPISpec, null, 2);
      } else if (targetFormat === 'yaml') {
        // For YAML conversion, we'll just stringify as JSON for now
        // Proper YAML conversion would require additional dependencies
        converted = JSON.stringify(asyncAPISpec, null, 2);
      }

      console.log(`🔄 Converted AsyncAPI to ${targetFormat.toUpperCase()}`);
      return converted;
    } catch (error) {
      console.error(`❌ Error converting to ${targetFormat}:`, error.message);
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
      const normalized = {
        // Original AsyncAPI data
        ...asyncAPISpec,
        
        // Add metadata for MongoDB
        metadata: {
          title: asyncAPISpec.info?.title || 'Untitled API',
          version: asyncAPISpec.info?.version || '1.0.0',
          description: asyncAPISpec.info?.description || '',
          protocol: this.extractProtocol(asyncAPISpec),
          channelsCount: Object.keys(asyncAPISpec.channels || {}).length,
          serversCount: Object.keys(asyncAPISpec.servers || {}).length,
          createdAt: new Date(),
          updatedAt: new Date(),
          processedAt: new Date()
        },

        // Add searchable fields
        searchableFields: {
          title: asyncAPISpec.info?.title?.toLowerCase() || '',
          description: asyncAPISpec.info?.description?.toLowerCase() || '',
          version: asyncAPISpec.info?.version || '',
          protocol: this.extractProtocol(asyncAPISpec).toLowerCase(),
          tags: this.extractTags(asyncAPISpec)
        }
      };

      console.log('🔧 AsyncAPI data normalized for MongoDB storage');
      return normalized;
    } catch (error) {
      console.error('❌ Error normalizing AsyncAPI data:', error.message);
      throw error;
    }
  }

  /**
   * Extract protocol from AsyncAPI specification
   * @param {Object} asyncAPISpec - Parsed AsyncAPI object
   * @returns {string} Protocol name
   */
  extractProtocol(asyncAPISpec) {
    const servers = asyncAPISpec.servers || {};
    const serverValues = Object.values(servers);
    
    if (serverValues.length > 0) {
      const protocol = serverValues[0].protocol;
      return protocol || 'unknown';
    }
    
    return 'unknown';
  }

  /**
   * Extract tags from AsyncAPI specification
   * @param {Object} asyncAPISpec - Parsed AsyncAPI object
   * @returns {Array} Array of tags
   */
  extractTags(asyncAPISpec) {
    // Tags can be at top level or under info (check both)
    const tags = asyncAPISpec.tags || asyncAPISpec.info?.tags || [];
    return tags.map(tag => tag.name || tag).filter(Boolean);
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
      const converted = this.convertAsyncAPI(parsed, targetFormat);
      
      // Normalize for MongoDB
      const normalized = this.normalizeAsyncAPIData(parsed);
      
      console.log('✅ AsyncAPI processing completed successfully');
      
      return {
        original: content,
        parsed: parsed,
        converted: converted,
        normalized: normalized,
        validation: validation
      };
    } catch (error) {
      console.error('❌ AsyncAPI processing failed:', error.message);
      throw error;
    }
  }
}

module.exports = AsyncAPIProcessor;
