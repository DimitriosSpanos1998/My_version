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

/** helpers */
const asArray = (x) => (Array.isArray(x) ? x : x == null ? [] : [x]);
const cleanStr = (s) => (typeof s === 'string' ? s : undefined);

function extractTags(spec, info) {
  const raw = Array.isArray(spec.tags) ? spec.tags : Array.isArray(info?.tags) ? info.tags : [];
  return raw
    .map((t) => (typeof t === 'string' ? { name: t } : t))
    .filter(Boolean)
    .map((t) => ({
      name: t.name,
      description: cleanStr(t.description),
      externalDocs: t.externalDocs?.url
        ? { url: t.externalDocs.url, description: t.externalDocs.description }
        : undefined
    }));
}

function extractServers(spec) {
  const serversObj = spec.servers || {};
  return Object.entries(serversObj).map(([name, s]) => ({
    name,
    url: s?.url,
    protocol: s?.protocol,
    protocolVersion: s?.protocolVersion,
    description: s?.description,
    security: asArray(s?.security)
      .map((x) => (typeof x === 'object' ? Object.keys(x)[0] : x))
      .filter(Boolean),
    variables: s?.variables
      ? Object.entries(s.variables).map(([vn, v]) => ({
          name: vn,
          default: v?.default,
          enum: Array.isArray(v?.enum) ? v.enum : undefined,
          description: v?.description
        }))
      : undefined,
    bindings: s?.bindings || undefined
  }));
}

/** v2 publish/subscribe πάνω στο channel */
function extractOpsV2(channelsObj) {
  return Object.entries(channelsObj || {}).flatMap(([chName, ch]) => {
    const out = [];
    if (ch?.publish) {
      out.push({
        action: 'publish',
        operationId: ch.publish.operationId,
        channel: chName,
        summary: ch.publish.summary,
        description: ch.publish.description,
        message: ch.publish.message
      });
    }
    if (ch?.subscribe) {
      out.push({
        action: 'subscribe',
        operationId: ch.subscribe.operationId,
        channel: chName,
        summary: ch.subscribe.summary,
        description: ch.subscribe.description,
        message: ch.subscribe.message
      });
    }
    return out;
  });
}

/** v3 operations */
function extractOpsV3(spec) {
  // δύο μορφές: spec.operations (map/array) ή channel.operations
  const fromRoot = Array.isArray(spec.operations)
    ? spec.operations.map((op) => ({
        action: op?.action, // send | receive
        operationId: op?.operationId,
        channel: typeof op?.channel === 'string' ? op.channel : op?.channel?.$ref || op?.channel?.name,
        summary: op?.summary,
        description: op?.description,
        messages: op?.messages
      }))
    : Object.entries(spec.operations || {}).map(([key, op]) => ({
        action: op?.action,
        operationId: op?.operationId || key,
        channel: typeof op?.channel === 'string' ? op.channel : op?.channel?.$ref || op?.channel?.name,
        summary: op?.summary,
        description: op?.description,
        messages: op?.messages
      }));

  const fromChannels = Object.entries(spec.channels || {}).flatMap(([chName, ch]) => {
    const ops = [];
    if (Array.isArray(ch?.operations)) {
      for (const op of ch.operations) {
        ops.push({
          action: op?.action,
          operationId: op?.operationId,
          channel: chName,
          summary: op?.summary,
          description: op?.description,
          messages: op?.messages
        });
      }
    } else if (ch?.operations && typeof ch.operations === 'object') {
      for (const [action, op] of Object.entries(ch.operations)) {
        ops.push({
          action,
          operationId: op?.operationId,
          channel: chName,
          summary: op?.summary,
          description: op?.description,
          messages: op?.messages
        });
      }
    }
    return ops;
  });

  return [...fromRoot, ...fromChannels];
}

function normalizeMessagesFromOp(asyncapi, op) {
  const raw = Array.isArray(op?.messages)
    ? op.messages
    : op?.message
      ? asArray(op.message)
      : [];

  return raw.map((m) => ({
    name: m?.name,
    title: m?.title || m?.name,
    summary: m?.summary,
    contentType: m?.contentType || asyncapi?.defaultContentType,
    schemaFormat: m?.schemaFormat,
    payload: m?.payload,
    headers: m?.headers,
    bindings: m?.bindings
  }));
}

function extractChannels(spec) {
  const channelsObj = spec.channels || {};
  return Object.entries(channelsObj).map(([name, ch]) => ({
    name,
    description: ch?.description,
    servers: Array.isArray(ch?.servers) ? ch.servers : undefined, // v3 optional
    parameters: ch?.parameters ? Object.keys(ch.parameters) : undefined,
    bindings: ch?.bindings || undefined
  }));
}

function extractComponents(spec) {
  const cmp = spec.components || {};
  const pick = (obj) => (obj ? Object.keys(obj) : []);
  return {
    has: Object.keys(cmp).length > 0 ? true : undefined,
    schemas: pick(cmp.schemas),
    messages: pick(cmp.messages),
    securitySchemes: pick(cmp.securitySchemes),
    parameters: pick(cmp.parameters),
    correlationIds: pick(cmp.correlationIds),
    operationTraits: pick(cmp.operationTraits),
    messageTraits: pick(cmp.messageTraits),
    serverBindings: pick(cmp.serverBindings),
    channelBindings: pick(cmp.channelBindings),
    operationBindings: pick(cmp.operationBindings),
    messageBindings: pick(cmp.messageBindings)
  };
}

function extractSecurity(spec) {
  const sec = spec.components?.securitySchemes || {};
  return Object.entries(sec).map(([name, s]) => {
    const out = { name, type: s?.type };
    if (s?.type === 'http') {
      out.scheme = s.scheme;
      out.bearerFormat = s.bearerFormat;
    }
    if (s?.type === 'oauth2') {
      out.flows = Object.fromEntries(
        Object.entries(s.flows || {}).map(([fname, flow]) => [
          fname,
          {
            authorizationUrl: flow.authorizationUrl,
            tokenUrl: flow.tokenUrl,
            refreshUrl: flow.refreshUrl,
            scopes: Object.keys(flow.scopes || {})
          }
        ])
      );
    }
    if (s?.type === 'openIdConnect') {
      out.openIdConnectUrl = s.openIdConnectUrl;
    }
    out.description = s?.description;
    return out;
  });
}

/**
 * Κύρια συνάρτηση: spec → { AsyncService: [ { ... } ] }
 */
function buildAsyncService(spec, explicitId) {
  const info = spec.info || {};
  let id = explicitId || spec.id || spec['x-id'];

  if (!id) {
    const hashInput = JSON.stringify({
      title: info.title || '',
      version: info.version || '',
      asyncapi: spec.asyncapi || spec.version || '',
      description: info.description || '',
      servers: spec.servers || {},
      channels: spec.channels || {}
    });
    id = `asyncapi-${crypto.createHash('sha256').update(hashInput).digest('hex').slice(0, 32)}`;
  }

  if (!id) {
    id = uuidv4();
  }

  // Servers / Channels
  const Server = extractServers(spec);
  const Channel = extractChannels(spec);

  // Operations (v2 + v3), και flatten messages (unique by title/name)
  const ops = [...extractOpsV3(spec), ...extractOpsV2(spec.channels || {})];
  const Message = [];
  for (const op of ops) {
    const msgs = normalizeMessagesFromOp(spec, op);
    for (const m of msgs) {
      // push χωρίς dedupe για αρχή (ή χρησιμοποίησε map με key `${m.title}|${m.contentType}`)
      Message.push({
        title: m.title,
        name: m.name,
        summary: m.summary,
        contentType: m.contentType,
        schemaFormat: m.schemaFormat
      });
    }
  }

  // Components / Security / Tag
  const Component = extractComponents(spec);
  const Security = extractSecurity(spec);
  const Tag = extractTags(spec, info);

  const AsyncService = [
    {
      id,
      title: info.title || 'Untitled API',
      version: info.version || '',
      description: info.description || '',
      defaultContentType: spec.defaultContentType,
      asyncapiVersion: spec.asyncapi || spec.version,
      termsOfService: info.termsOfService,
      contactName: info.contact?.name,
      contactEmail: info.contact?.email,
      contactUrl: info.contact?.url,
      licenseName: info.license?.name,
      licenseUrl: info.license?.url,
      externalDocsDescription: spec.externalDocs?.description,
      externalDocsUrl: spec.externalDocs?.url,
      // sub-tables
      Server,
      Channel,
      Component,
      Tag,
      Security,
      // προαιρετικά: αν θες να συμπεριλάβεις το collection των messages
      Message
    }
  ];

  return { AsyncService };
}

module.exports = { buildAsyncService };
