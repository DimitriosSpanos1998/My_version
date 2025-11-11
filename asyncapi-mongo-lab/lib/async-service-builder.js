const crypto = require('crypto');

const uuidv4 = () => {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }

  const bytes = crypto.randomBytes(16);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};

const asArray = (value) => (Array.isArray(value) ? value : value == null ? [] : [value]);
const cleanStr = (value) => (typeof value === 'string' ? value : undefined);

const decodePointerSegment = (segment) => segment.replace(/~1/g, '/').replace(/~0/g, '~');

const resolveJsonPointer = (root, ref) => {
  if (typeof ref !== 'string' || !ref.startsWith('#/')) {
    return undefined;
  }

  const segments = ref
    .slice(2)
    .split('/')
    .map(decodePointerSegment)
    .filter(Boolean);

  let current = root;
  for (const segment of segments) {
    if (current == null || typeof current !== 'object') {
      return undefined;
    }
    current = current[segment];
  }

  return current;
};

const sanitize = (value) => {
  if (Array.isArray(value)) {
    return value
      .map((item) => sanitize(item))
      .filter((item) => {
        if (item == null) return false;
        if (typeof item === 'object' && !Array.isArray(item) && Object.keys(item).length === 0) {
          return false;
        }
        return true;
      });
  }

  if (value && typeof value === 'object') {
    const result = {};
    for (const [key, val] of Object.entries(value)) {
      const sanitized = sanitize(val);
      if (sanitized === undefined) {
        continue;
      }
      if (Array.isArray(sanitized) && sanitized.length === 0) {
        result[key] = sanitized;
        continue;
      }
      if (sanitized && typeof sanitized === 'object' && !Array.isArray(sanitized) && Object.keys(sanitized).length === 0) {
        continue;
      }
      result[key] = sanitized;
    }
    return result;
  }

  return value === undefined ? undefined : value;
};

const resolveMessageObject = (spec, message) => {
  if (!message || typeof message !== 'object') {
    return message;
  }

  if (message.$ref) {
    const resolved = resolveJsonPointer(spec, message.$ref);
    if (resolved && resolved !== message) {
      return resolveMessageObject(spec, resolved);
    }
  }

  return message;
};

const collectServerSecurity = (security) =>
  asArray(security)
    .map((entry) => {
      if (!entry) return null;
      if (typeof entry === 'string') {
        return { scheme: entry };
      }
      if (typeof entry === 'object') {
        const [[scheme, scopes]] = Object.entries(entry);
        return sanitize({ scheme, scopes: Array.isArray(scopes) ? scopes : [] });
      }
      return null;
    })
    .filter(Boolean);

const collectServerVariables = (variables = {}) =>
  Object.entries(variables).map(([name, variable]) =>
    sanitize({
      name,
      default: variable?.default,
      enum: Array.isArray(variable?.enum) ? variable.enum : undefined,
      description: cleanStr(variable?.description)
    })
  );

const extractServers = (spec) =>
  Object.entries(spec.servers || {}).map(([name, server]) =>
    sanitize({
      name,
      url: cleanStr(server?.url),
      protocol: cleanStr(server?.protocol),
      protocolVersion: cleanStr(server?.protocolVersion),
      description: cleanStr(server?.description),
      security: collectServerSecurity(server?.security),
      variables: collectServerVariables(server?.variables),
      bindings: server?.bindings || undefined
    })
  );

const collectChannelParameters = (parameters = {}) =>
  Object.entries(parameters).map(([name, parameter]) =>
    sanitize({
      name,
      description: cleanStr(parameter?.description),
      location: cleanStr(parameter?.location),
      schema: parameter?.schema,
      examples: Array.isArray(parameter?.examples) ? parameter.examples : undefined
    })
  );

const extractChannels = (spec) =>
  Object.entries(spec.channels || {}).map(([name, channel]) => {
    const parameters = collectChannelParameters(channel?.parameters || {});
    return sanitize({
      name,
      description: cleanStr(channel?.description),
      servers: Array.isArray(channel?.servers) ? channel.servers : undefined,
      parameters: parameters.length ? parameters : undefined,
      bindings: channel?.bindings || undefined
    });
  });

const extractTags = (spec) => {
  const infoTags = Array.isArray(spec.info?.tags) ? spec.info.tags : [];
  const rootTags = Array.isArray(spec.tags) ? spec.tags : [];
  const combined = [...rootTags, ...infoTags];
  const registry = new Map();

  combined.forEach((tag) => {
    if (!tag) return;
    if (typeof tag === 'string') {
      registry.set(tag, { name: tag });
      return;
    }

    const name = cleanStr(tag.name);
    if (!name) return;

    const entry = sanitize({
      name,
      description: cleanStr(tag.description),
      externalDocs: tag.externalDocs?.url
        ? sanitize({
            url: cleanStr(tag.externalDocs.url),
            description: cleanStr(tag.externalDocs.description)
          })
        : undefined
    });

    registry.set(name, entry);
  });

  return Array.from(registry.values());
};

const extractComponents = (spec) => {
  const components = spec.components || {};
  const sections = [
    'schemas',
    'messages',
    'securitySchemes',
    'parameters',
    'correlationIds',
    'operationTraits',
    'messageTraits',
    'serverBindings',
    'channelBindings',
    'operationBindings',
    'messageBindings'
  ];

  const result = {};

  sections.forEach((section) => {
    const definition = components[section];
    const keys = definition && typeof definition === 'object' ? Object.keys(definition) : [];
    if (keys.length) {
      result[section] = keys;
    }
  });

  if (Object.keys(result).length === 0) {
    return {};
  }

  return { has: true, ...result };
};

const extractSecuritySchemes = (spec) => {
  const schemes = spec.components?.securitySchemes || {};

  return Object.entries(schemes).map(([name, scheme]) => {
    const flowsSource = scheme?.flows && typeof scheme.flows === 'object' ? scheme.flows : {};
    const flows = Object.entries(flowsSource).reduce((acc, [flowName, flow]) => {
      acc[flowName] = sanitize({
        authorizationUrl: cleanStr(flow?.authorizationUrl),
        tokenUrl: cleanStr(flow?.tokenUrl),
        refreshUrl: cleanStr(flow?.refreshUrl),
        scopes: Object.keys(flow?.scopes || {})
      });
      return acc;
    }, {});

    return sanitize({
      name,
      type: cleanStr(scheme?.type),
      description: cleanStr(scheme?.description),
      scheme: cleanStr(scheme?.scheme),
      bearerFormat: cleanStr(scheme?.bearerFormat),
      openIdConnectUrl: cleanStr(scheme?.openIdConnectUrl),
      in: cleanStr(scheme?.in),
      keyName: cleanStr(scheme?.name),
      flows: Object.keys(flows).length ? flows : undefined
    });
  });
};

const normalizeChannelReference = (channel) => {
  if (typeof channel === 'string') {
    return channel;
  }

  if (channel?.name) {
    return channel.name;
  }

  if (channel?.$ref) {
    const ref = channel.$ref;
    if (ref.startsWith('#/channels/')) {
      const suffix = ref.slice('#/channels/'.length);
      return suffix
        .split('/')
        .map(decodePointerSegment)
        .join('/');
    }
    return ref;
  }

  return undefined;
};

const extractOperationMessages = (operation) => {
  if (!operation || typeof operation !== 'object') {
    return [];
  }

  if (Array.isArray(operation.messages)) {
    return operation.messages;
  }

  if (operation.message != null) {
    return asArray(operation.message);
  }

  return [];
};

const collectOperations = (spec) => {
  const operations = [];
  const rootOperations = spec.operations;

  if (Array.isArray(rootOperations)) {
    rootOperations.forEach((operation) => {
      operations.push({
        action: cleanStr(operation?.action),
        operationId: cleanStr(operation?.operationId),
        channel: normalizeChannelReference(operation?.channel),
        summary: cleanStr(operation?.summary),
        description: cleanStr(operation?.description),
        messages: extractOperationMessages(operation)
      });
    });
  } else if (rootOperations && typeof rootOperations === 'object') {
    Object.entries(rootOperations).forEach(([operationId, operation]) => {
      operations.push({
        action: cleanStr(operation?.action),
        operationId: cleanStr(operation?.operationId) || cleanStr(operationId),
        channel: normalizeChannelReference(operation?.channel),
        summary: cleanStr(operation?.summary),
        description: cleanStr(operation?.description),
        messages: extractOperationMessages(operation)
      });
    });
  }

  Object.entries(spec.channels || {}).forEach(([channelName, channel]) => {
    if (Array.isArray(channel?.operations)) {
      channel.operations.forEach((operation) => {
        operations.push({
          action: cleanStr(operation?.action),
          operationId: cleanStr(operation?.operationId),
          channel: channelName,
          summary: cleanStr(operation?.summary),
          description: cleanStr(operation?.description),
          messages: extractOperationMessages(operation)
        });
      });
    } else if (channel?.operations && typeof channel.operations === 'object') {
      Object.entries(channel.operations).forEach(([action, operation]) => {
        operations.push({
          action: cleanStr(operation?.action) || cleanStr(action),
          operationId: cleanStr(operation?.operationId),
          channel: channelName,
          summary: cleanStr(operation?.summary),
          description: cleanStr(operation?.description),
          messages: extractOperationMessages(operation)
        });
      });
    }

    if (channel?.publish) {
      operations.push({
        action: 'publish',
        operationId: cleanStr(channel.publish.operationId),
        channel: channelName,
        summary: cleanStr(channel.publish.summary),
        description: cleanStr(channel.publish.description),
        messages: extractOperationMessages(channel.publish)
      });
    }

    if (channel?.subscribe) {
      operations.push({
        action: 'subscribe',
        operationId: cleanStr(channel.subscribe.operationId),
        channel: channelName,
        summary: cleanStr(channel.subscribe.summary),
        description: cleanStr(channel.subscribe.description),
        messages: extractOperationMessages(channel.subscribe)
      });
    }
  });

  return operations;
};

const createMessageEntry = (spec, message, fallbackName, defaultContentType) => {
  if (!message || typeof message !== 'object') {
    return null;
  }

  const resolved = resolveMessageObject(spec, message);
  const source = resolved && resolved !== message ? { ...resolved, ...message } : message;

  const entry = sanitize({
    name: cleanStr(source.name) || cleanStr(fallbackName),
    title: cleanStr(source.title) || cleanStr(source.name) || cleanStr(fallbackName),
    summary: cleanStr(source.summary),
    messageId: cleanStr(source.messageId),
    contentType: cleanStr(source.contentType) || cleanStr(defaultContentType),
    schemaFormat: cleanStr(source.schemaFormat),
    correlationId: source.correlationId,
    headers: source.headers,
    payload: source.payload,
    bindings: source.bindings,
    traits: Array.isArray(source.traits) ? source.traits : undefined,
    examples: Array.isArray(source.examples) ? source.examples : undefined
  });

  if (!entry || Object.keys(entry).length === 0) {
    return null;
  }

  return entry;
};

const extractMessages = (spec) => {
  const operations = collectOperations(spec);
  const defaultContentType = cleanStr(spec.defaultContentType);
  const registry = new Map();

  const addMessage = (message, fallbackName) => {
    const entry = createMessageEntry(spec, message, fallbackName, defaultContentType);
    if (!entry) {
      return;
    }

    const key = [entry.name, entry.title, entry.contentType].filter(Boolean).join('|');
    if (!registry.has(key)) {
      registry.set(key, entry);
    }
  };

  const componentMessages = spec.components?.messages || {};
  Object.entries(componentMessages).forEach(([name, message]) => {
    const fallbackName = cleanStr(name);
    addMessage({ ...message, name: message?.name || fallbackName }, fallbackName);
  });

  operations.forEach((operation) => {
    extractOperationMessages(operation).forEach((message) => addMessage(message));
  });

  return Array.from(registry.values());
};

const buildAsyncService = (spec = {}, explicitId) => {
  const info = spec.info || {};
  const asyncapiVersion = cleanStr(spec.asyncapi || spec.version);

  const service = {
    id: explicitId || spec.id || spec['x-id'] || uuidv4(),
    title: cleanStr(info.title) || 'Untitled API',
    version: cleanStr(info.version) || '',
    description: cleanStr(info.description) || '',
    defaultContentType: cleanStr(spec.defaultContentType),
    asyncapiVersion,
    termsOfService: cleanStr(info.termsOfService),
    contactName: cleanStr(info.contact?.name),
    contactEmail: cleanStr(info.contact?.email),
    contactUrl: cleanStr(info.contact?.url),
    licenseName: cleanStr(info.license?.name),
    licenseUrl: cleanStr(info.license?.url),
    externalDocsDescription: cleanStr(spec.externalDocs?.description),
    externalDocsUrl: cleanStr(spec.externalDocs?.url),
    Server: extractServers(spec),
    Channel: extractChannels(spec),
    Component: extractComponents(spec),
    Tag: extractTags(spec),
    Security: extractSecuritySchemes(spec),
    Message: extractMessages(spec)
  };

  return { AsyncService: [sanitize(service)] };
};

module.exports = { buildAsyncService };
