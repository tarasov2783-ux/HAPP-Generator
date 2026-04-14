// server.js (ФИНАЛЬНАЯ ВЕРСИЯ)

import express from 'express';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { createHappCryptoLink } from '@kastov/cryptohapp';
import { Low } from 'lowdb';
import { JSONFile } from 'lowdb/node';
import UAParser from 'ua-parser-js';
import QRCode from 'qrcode';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

app.set('trust proxy', true);

const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASS = process.env.ADMIN_PASS || 'changeme123';
const APPSTORE_URL = 'https://apps.apple.com/ru/app/happ-proxy-utility-plus/id6746188973';
const PLAYSTORE_URL = 'https://play.google.com/store/apps/details?id=com.happproxy&hl=ru';
const WINDOWS_DOWNLOAD_URL = 'https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe';
const MACOS_DOWNLOAD_URL = 'https://github.com/Happ-proxy/happ-desktop/releases/latest/download/Happ-x64.dmg';
const LINUX_DOWNLOAD_URL = 'https://github.com/Happ-proxy/happ-desktop/releases/latest/download/happ-desktop-x86_64.AppImage';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const adapter = new JSONFile(path.join(__dirname, 'db.json'));
const db = new Low(adapter, { links: [] });
await db.read();
db.data ||= { links: [] };

// Система блокировок
const linkLocks = new Map(); // для отдельных ссылок
const groupLocks = new Map(); // для групп ссылок (по subscriptionUrl)
// Кэш для предотвращения дубликатов (только для одинаковых запросов)
const requestCache = new Map();

function makeId(size = 16) {
  return crypto.randomBytes(size).toString('hex');
}

function normalizeInput(value = '') {
  let v = String(value || '').trim();
  if (!v) return '';

  v = v.replace(/\u200B/g, '').trim();

  const markdownMatch = v.match(/^\[([^\]]+)\]\((https?:\/\/[^)]+)\)$/i);
  if (markdownMatch) v = markdownMatch[2].trim();

  const angleMatch = v.match(/^<\s*(https?:\/\/[^>]+)\s*>$/i);
  if (angleMatch) v = angleMatch[1].trim();

  v = v.replace(/^["']+|["']+$/g, '').trim();

  if (!/^https?:\/\//i.test(v) && /^[a-z0-9.-]+\.[a-z]{2,}([/:?#].*)?$/i.test(v)) {
    v = `https://${v}`;
  }

  return v;
}

function isValidHttpUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

function extractUsernameFromSubscriptionUrl(value) {
  try {
    const url = new URL(value);
    const parts = url.pathname.split('/').filter(Boolean);
    return parts[parts.length - 1] || '';
  } catch {
    return '';
  }
}

function getClientIp(req) {
  const forwarded = req.headers['x-forwarded-for'];
  if (forwarded) return String(forwarded).split(',')[0].trim();
  return req.socket?.remoteAddress || '';
}

function normalizeIp(u = {}) {
  const ip = String(u.ip || '').trim();
  return ip.startsWith('::ffff:') ? ip.slice(7) : ip;
}

function detectWindowsVersion(ua, result) {
  const uaLower = ua.toLowerCase();
  const osName = result.os?.name || '';
  const osVersion = result.os?.version || '';

  if (osName !== 'Windows') {
    return { osName, osVersion };
  }

  if (osVersion === '10') {
    const isModernBrowser =
      (result.browser?.name === 'Chrome' && parseInt(result.browser?.version || '0') >= 100) ||
      (result.browser?.name === 'Edge' && parseInt(result.browser?.version || '0') >= 90) ||
      (result.browser?.name === 'Firefox' && parseInt(result.browser?.version || '0') >= 100);

    const is64Bit = uaLower.includes('win64') || uaLower.includes('x64');
    const isNotOlderVersion = !uaLower.includes('windows 6.') && !uaLower.includes('windows nt 6.');

    if (isModernBrowser && is64Bit && isNotOlderVersion) {
      return { osName: 'Windows', osVersion: '11' };
    }
  }

  return { osName, osVersion };
}

function parseClient(req) {
  const ua = req.headers['user-agent'] || '';
  const parser = new UAParser(ua);
  const result = parser.getResult();
  const body = req.body || {};

  const { osName, osVersion } = detectWindowsVersion(ua, result);

  return {
    ip: getClientIp(req),
    userAgent: ua,
    browser: [result.browser?.name, result.browser?.version].filter(Boolean).join(' ') || '',
    browserName: result.browser?.name || '',
    browserVersion: result.browser?.version || '',
    os: body.os || (osName && osVersion ? `${osName} ${osVersion}` : result.os?.name || ''),
    osName: osName || result.os?.name || '',
    osVersion: osVersion || result.os?.version || '',
    deviceType: result.device?.type || body.deviceType || 'desktop',
    deviceVendor: result.device?.vendor || '',
    deviceModel: result.device?.model || '',
    platform: body.platform || '',
    language: body.language || '',
    languages: Array.isArray(body.languages) ? body.languages.join(',') : '',
    screen: body.screen || '',
    timezone: body.timezone || '',
    clientId: body.clientId || '',
    pageSessionId: body.pageSessionId || '',
    hardwareConcurrency: body.hardwareConcurrency || '',
    deviceMemory: body.deviceMemory || '',
    colorDepth: body.colorDepth || '',
    pixelRatio: body.pixelRatio || '',
    touchPoints: body.touchPoints || '',
    viewport: body.viewport || '',
    referrer: body.referrer || '',
    pageUrl: body.pageUrl || '',
    pagePath: body.pagePath || ''
  };
}

function makeDeviceKey(u = {}) {
  const clientId = String(u.clientId || '').slice(-12);
  const osName = u.osName || 'unknown';
  const osVersion = u.osVersion ? ` ${u.osVersion}` : '';
  const browserName = u.browserName || 'unknown';
  const browserVersion = u.browserVersion ? ` ${u.browserVersion}` : '';
  const screen = u.screen || 'unknown';
  const timezone = u.timezone || 'unknown';

  if (osName === 'Windows') {
    return `windows|${clientId}|${osVersion.trim()}|${browserName}${browserVersion}|${screen}|${timezone}`;
  }

  if (osName === 'iOS' || osName === 'macOS') {
    return `apple|${clientId}|${osName}${osVersion}|${browserName}${browserVersion}|${screen}|${timezone}`;
  }

  if (osName === 'Android') {
    return `android|${clientId}|${osVersion.trim()}|${browserName}${browserVersion}|${screen}|${timezone}`;
  }

  return `${osName}${osVersion}|${clientId}|${browserName}${browserVersion}|${screen}|${timezone}`;
}

function makeRawDeviceKey(u = {}) {
  return [
    u.clientId || '',
    normalizeIp(u),
    u.browser || '',
    u.browserVersion || '',
    u.os || '',
    u.osVersion || '',
    u.platform || '',
    u.language || '',
    u.screen || '',
    u.timezone || '',
    u.deviceType || '',
    u.deviceVendor || '',
    u.deviceModel || '',
    u.hardwareConcurrency || '',
    u.deviceMemory || '',
    u.colorDepth || '',
    u.pixelRatio || '',
    u.touchPoints || '',
    u.viewport || ''
  ].join('|');
}

function getPrimaryDeviceKey(item) {
  if (!item.activations || !item.activations.length) return null;
  return item.activations[0].deviceKey;
}

function getPrimaryUsage(item) {
  if (!item.activations || !item.activations.length) return null;
  return item.activations[0];
}

function getGroupLinks(subscriptionUrl) {
  return (db.data.links || []).filter(x => x.subscriptionUrl === subscriptionUrl);
}

function getGroupPrimaryDeviceKey(subscriptionUrl) {
  const links = getGroupLinks(subscriptionUrl);

  for (const link of links) {
    if (link.activations && link.activations.length > 0) {
      return link.activations[0].deviceKey;
    }
  }

  return null;
}

function getGroupPrimaryUsage(subscriptionUrl) {
  const links = getGroupLinks(subscriptionUrl);

  for (const link of links) {
    if (link.activations && link.activations.length > 0) {
      return link.activations[0];
    }
  }

  return null;
}

function hasGroupActivations(subscriptionUrl) {
  const links = getGroupLinks(subscriptionUrl);
  return links.some(link => link.activations && link.activations.length > 0);
}

function beautifyPlatform(u = {}) {
  const p = u.osName || '';
  if (p === 'iOS') return 'iPhone / iPad';
  if (p === 'Android') return 'Android';
  if (p === 'Windows') return 'Windows';
  if (p === 'macOS') return 'macOS';
  if (p === 'Linux') return 'Linux';
  return 'Другое устройство';
}

function beautifyDeviceType(u = {}) {
  const type = String(u.deviceType || '').toLowerCase();

  if (type === 'tablet') return 'Планшет';
  if (type === 'mobile') return 'Телефон';
  if (type === 'smarttv') return 'TV';
  if (type === 'wearable') return 'Носимое устройство';

  const platform = u.osName || '';
  if (platform === 'Android' || platform === 'iOS') return 'Мобильное устройство';

  return 'Компьютер';
}

function beautifyBrowser(u = {}) {
  return u.browser || 'Неизвестный браузер';
}

function beautifyOs(u = {}) {
  return u.os || 'Неизвестная ОС';
}

function beautifyModel(u = {}) {
  const parts = [u.deviceVendor, u.deviceModel].filter(Boolean).join(' ').trim();
  return parts || '';
}

function buildUserFacingDeviceInfo(usage) {
  if (!usage) return null;

  return {
    title: `${beautifyPlatform(usage)} • ${beautifyDeviceType(usage)}`,
    platform: beautifyPlatform(usage),
    deviceType: beautifyDeviceType(usage),
    os: beautifyOs(usage),
    browser: beautifyBrowser(usage),
    model: beautifyModel(usage),
    screen: usage.screen || '',
    timezone: usage.timezone || '',
    language: usage.language || '',
    firstSeenAt: usage.at || '',
    lastSeenAt: usage.at || '',
    deviceKey: usage.deviceKey,
    ip: usage.ip ? usage.ip.replace('::ffff:', '') : ''
  };
}

function getBoundDeviceInfoForSubscription(subscriptionUrl) {
  const usage = getGroupPrimaryUsage(subscriptionUrl);
  return buildUserFacingDeviceInfo(usage);
}

function isDuplicateRequest(token, clientId, pageSessionId) {
  const key = `${token}:${clientId}:${pageSessionId}`;
  const now = Date.now();

  if (requestCache.has(key)) {
    const lastTime = requestCache.get(key);
    if (now - lastTime < 5000) { // Уменьшил до 5 секунд
      return true;
    }
  }

  requestCache.set(key, now);

  for (const [k, time] of requestCache.entries()) {
    if (now - time > 30000) {
      requestCache.delete(k);
    }
  }

  return false;
}

async function saveDb() {
  await db.write();
}

function sendJson(res, status, payload) {
  return res
    .status(status)
    .type('application/json; charset=utf-8')
    .send(JSON.stringify(payload));
}

function requireAdmin(req, res, next) {
  const authorization = req.headers.authorization;

  if (!authorization || !authorization.startsWith('Basic ')) {
    res.setHeader('WWW-Authenticate', 'Basic realm="HAPP Admin"');
    return res.status(401).send('Authentication required');
  }

  const decoded = Buffer.from(authorization.replace('Basic ', ''), 'base64').toString();
  const [user, pass] = decoded.split(':');

  if (user !== ADMIN_USER || pass !== ADMIN_PASS) {
    res.setHeader('WWW-Authenticate', 'Basic realm="HAPP Admin"');
    return res.status(401).send('Invalid credentials');
  }

  next();
}

async function withLinkLock(token, fn) {
  if (!linkLocks.has(token)) {
    linkLocks.set(token, Promise.resolve());
  }

  const previous = linkLocks.get(token);
  const current = previous.then(() => fn()).finally(() => {
    if (linkLocks.get(token) === current) {
      linkLocks.delete(token);
    }
  });

  linkLocks.set(token, current);
  return current;
}

async function withGroupLock(subscriptionUrl, fn) {
  if (!groupLocks.has(subscriptionUrl)) {
    groupLocks.set(subscriptionUrl, Promise.resolve());
  }

  const previous = groupLocks.get(subscriptionUrl);
  const current = previous.then(() => fn()).finally(() => {
    if (groupLocks.get(subscriptionUrl) === current) {
      groupLocks.delete(subscriptionUrl);
    }
  });

  groupLocks.set(subscriptionUrl, current);
  return current;
}

function mapLinkForAdmin(item) {
  const activations = item.activations || [];
  const violations = item.violations || [];
  const primaryDeviceKey = activations[0]?.deviceKey || null;

  const sameDeviceCount = activations.length;
  const foreignDeviceCount = violations.length;
  const uniqueDevices = new Set([
    ...activations.map(a => a.deviceKey),
    ...violations.map(v => v.deviceKey)
  ]).size;

  // totalUsed = успешные + нарушения
  const totalUsed = activations.length + violations.length;

  return {
    id: item.id,
    token: item.token,
    username: item.username,
    subscriptionUrl: item.subscriptionUrl,
    happLink: item.happLink,
    maxActivations: item.maxActivations,
    usedCount: totalUsed, // Показываем ВСЕ использования
    remaining: Math.max(item.maxActivations - totalUsed, 0),
    status: item.status,
    createdAt: item.createdAt,
    lastUsedAt: item.lastUsedAt,
    primaryDeviceKey,
    uniqueDevices,
    sameDeviceCount,
    foreignDeviceCount,
    isViolator: violations.length > 0 || item.status === 'violator',
    activations,
    violations
  };
}

function buildGroups(links) {
  const groupsMap = new Map();

  for (const item of links) {
    const key = item.subscriptionUrl || '__empty__';

    if (!groupsMap.has(key)) {
      groupsMap.set(key, {
        subscriptionUrl: item.subscriptionUrl,
        username: item.username,
        links: [],
        maxActivationsTotal: 0,
        usedCountTotal: 0,
        allActivations: [],
        allViolations: [],
        violatorLinks: 0,
        lastUsedAt: null
      });
    }

    const group = groupsMap.get(key);
    group.links.push(item);
    group.maxActivationsTotal += Number(item.maxActivations || 0);

    const activations = item.activations || [];
    const violations = item.violations || [];

    group.usedCountTotal += activations.length + violations.length; // ВСЕ использования
    group.allActivations.push(...activations);
    group.allViolations.push(...violations);

    if (violations.length > 0) {
      group.violatorLinks += 1;
    }

    if (!group.lastUsedAt || (item.lastUsedAt && item.lastUsedAt > group.lastUsedAt)) {
      group.lastUsedAt = item.lastUsedAt || group.lastUsedAt;
    }
  }

  return Array.from(groupsMap.values()).map(group => {
    const uniqueDevices = new Set([
      ...group.allActivations.map(a => a.deviceKey),
      ...group.allViolations.map(v => v.deviceKey)
    ]).size;

    const foreignDeviceCountTotal = group.allViolations.length;
    const isViolator = foreignDeviceCountTotal > 0 || group.violatorLinks > 0;
    const primaryDeviceKey = group.allActivations[0]?.deviceKey || null;
    const primaryUsage = group.allActivations[0] || null;

    return {
      subscriptionUrl: group.subscriptionUrl,
      username: group.username,
      linksCount: group.links.length,
      linkIds: group.links.map(x => x.id),
      tokens: group.links.map(x => x.token),
      maxActivationsTotal: group.maxActivationsTotal,
      usedCountTotal: group.usedCountTotal, // ВСЕ использования
      remainingTotal: Math.max(group.maxActivationsTotal - group.usedCountTotal, 0),
      uniqueDevices,
      violatorLinks: group.violatorLinks,
      foreignDeviceCountTotal,
      primaryDeviceKey,
      boundDevice: buildUserFacingDeviceInfo(primaryUsage),
      isViolator,
      lastUsedAt: group.lastUsedAt,
      links: group.links.map(mapLinkForAdmin)
    };
  });
}

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/admin.html', requireAdmin, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

app.post('/api/generate', async (req, res) => {
  const rawInput = typeof req.body?.subscriptionUrl === 'string'
    ? req.body.subscriptionUrl
    : String(req.body?.subscriptionUrl ?? '');

  const subscriptionUrl = normalizeInput(rawInput);
  const maxActivations = Number(req.body?.maxActivations || 1);

  if (!subscriptionUrl || !isValidHttpUrl(subscriptionUrl)) {
    return sendJson(res, 400, {
      ok: false,
      error: 'URL подписки не валиден'
    });
  }

  const username = extractUsernameFromSubscriptionUrl(subscriptionUrl);

  if (!username) {
    return sendJson(res, 400, {
      ok: false,
      error: 'Не удалось извлечь имя пользователя из ссылки'
    });
  }

  if (!Number.isInteger(maxActivations) || maxActivations < 1 || maxActivations > 100) {
    return sendJson(res, 400, {
      ok: false,
      error: 'Лимит активаций должен быть от 1 до 100'
    });
  }

  try {
    const happLink = createHappCryptoLink(subscriptionUrl, 'v4', true);

    if (!happLink || typeof happLink !== 'string' || !happLink.startsWith('happ://crypt')) {
      return sendJson(res, 500, {
        ok: false,
        error: 'Не удалось сгенерировать корректную happ-ссылку'
      });
    }

    const token = makeId(16);

    db.data.links.push({
      id: makeId(10),
      token,
      username,
      subscriptionUrl,
      happLink,
      maxActivations,
      usedCount: 0,
      status: 'active',
      createdAt: new Date().toISOString(),
      lastUsedAt: null,
      activations: [],
      violations: []
    });

    await saveDb();

    return sendJson(res, 200, {
      ok: true,
      onceLink: `/r/${token}`,
      username,
      maxActivations,
      happLink
    });
  } catch (e) {
    return sendJson(res, 500, {
      ok: false,
      error: e?.message || 'Ошибка генерации'
    });
  }
});

app.get('/api/link/:token', async (req, res) => {
  await db.read();

  const item = db.data.links.find(x => x.token === req.params.token);

  if (!item) {
    return sendJson(res, 404, {
      ok: false,
      error: 'Ссылка не найдена'
    });
  }

  const activations = item.activations || [];
  const violations = item.violations || [];
  const firstActivation = activations[0];
  
  // ВСЕ использования
  const totalUsed = activations.length + violations.length;
  const remaining = Math.max(item.maxActivations - totalUsed, 0);

  return sendJson(res, 200, {
    ok: true,
    username: item.username,
    usedCount: totalUsed, // Показываем ВСЕ использования
    maxActivations: item.maxActivations,
    remaining: remaining,
    status: item.status,
    appStoreUrl: APPSTORE_URL,
    playStoreUrl: PLAYSTORE_URL,
    windowsDownloadUrl: WINDOWS_DOWNLOAD_URL,
    macosDownloadUrl: MACOS_DOWNLOAD_URL,
    linuxDownloadUrl: LINUX_DOWNLOAD_URL,
    happLink: item.happLink,
    boundDevice: firstActivation ? {
      os: firstActivation.os,
      browser: firstActivation.browser,
      deviceType: firstActivation.deviceType,
      screen: firstActivation.screen,
      timezone: firstActivation.timezone,
      language: firstActivation.language,
      firstSeenAt: firstActivation.at,
      ip: firstActivation.ip
    } : null
  });
});

app.post('/api/check-device/:token', async (req, res) => {
  return withLinkLock(req.params.token, async () => {
    await db.read();

    const item = db.data.links.find(x => x.token === req.params.token);

    if (!item) {
      return sendJson(res, 404, {
        ok: false,
        error: 'Ссылка не найдена'
      });
    }

    const client = parseClient(req);
    const deviceKey = makeDeviceKey(client);

    const groupHasActivations = hasGroupActivations(item.subscriptionUrl);
    const groupPrimaryDeviceKey = getGroupPrimaryDeviceKey(item.subscriptionUrl);
    const activations = item.activations || [];
    const violations = item.violations || [];
    
    const totalUsed = activations.length + violations.length;
    const remaining = Math.max(item.maxActivations - totalUsed, 0);

    let status = 'ok';
    let message = null;
    let boundDevice = null;

    if (groupHasActivations) {
      if (groupPrimaryDeviceKey === deviceKey) {
        status = 'same-device';
        message = 'Это устройство уже активировало подписку';
      } else {
        status = 'different-device';
        message = 'Эта подписка уже активирована на другом устройстве';
        boundDevice = buildUserFacingDeviceInfo(getGroupPrimaryUsage(item.subscriptionUrl));
      }
    }

    return sendJson(res, 200, {
      ok: true,
      status,
      message,
      boundDevice,
      deviceKey,
      remaining: remaining,
      happLink: item.happLink,
      appStoreUrl: APPSTORE_URL,
      playStoreUrl: PLAYSTORE_URL,
      windowsDownloadUrl: WINDOWS_DOWNLOAD_URL,
      macosDownloadUrl: MACOS_DOWNLOAD_URL,
      linuxDownloadUrl: LINUX_DOWNLOAD_URL
    });
  });
});

app.post('/api/redeem-preview/:token', async (req, res) => {
  return withLinkLock(req.params.token, async () => {
    await db.read();

    const item = db.data.links.find(x => x.token === req.params.token);

    if (!item) {
      return sendJson(res, 404, {
        ok: false,
        error: 'Ссылка не найдена'
      });
    }

    const activations = item.activations || [];
    const violations = item.violations || [];
    
    const totalUsed = activations.length + violations.length;
    const remaining = Math.max(item.maxActivations - totalUsed, 0);

    if (totalUsed >= item.maxActivations) {
      item.status = 'used';
      await saveDb();
      return sendJson(res, 410, {
        ok: false,
        error: 'Лимит активаций исчерпан'
      });
    }

    if (!item.happLink || typeof item.happLink !== 'string' || !item.happLink.startsWith('happ://crypt')) {
      return sendJson(res, 500, {
        ok: false,
        error: 'Повреждённая happ-ссылка'
      });
    }

    const client = parseClient(req);
    const firstActivation = activations[0];

    return sendJson(res, 200, {
      ok: true,
      happLink: item.happLink,
      remaining: remaining,
      appStoreUrl: APPSTORE_URL,
      playStoreUrl: PLAYSTORE_URL,
      windowsDownloadUrl: WINDOWS_DOWNLOAD_URL,
      macosDownloadUrl: MACOS_DOWNLOAD_URL,
      linuxDownloadUrl: LINUX_DOWNLOAD_URL,
      boundDevice: firstActivation ? {
        os: firstActivation.os,
        browser: firstActivation.browser,
        deviceType: firstActivation.deviceType,
        screen: firstActivation.screen,
        timezone: firstActivation.timezone,
        language: firstActivation.language,
        firstSeenAt: firstActivation.at,
        ip: firstActivation.ip
      } : null
    });
  });
});

app.post('/api/redeem-confirm/:token', async (req, res) => {
  // Сначала получаем ссылку, чтобы узнать subscriptionUrl
  await db.read();
  const initialItem = db.data.links.find(x => x.token === req.params.token);
  
  if (!initialItem) {
    return sendJson(res, 404, { ok: false, error: 'Ссылка не найдена' });
  }
  
  // Блокируем всю группу ссылок с одинаковым subscriptionUrl
  return withGroupLock(initialItem.subscriptionUrl, async () => {
    // Перечитываем данные после получения блокировки группы
    await db.read();
    
    const item = db.data.links.find(x => x.token === req.params.token);
    
    if (!item) {
      return sendJson(res, 404, { ok: false, error: 'Ссылка не найдена' });
    }

    const client = parseClient(req);
    const deviceKey = makeDeviceKey(client);
    const rawDeviceKey = makeRawDeviceKey(client);
    const now = new Date().toISOString();

    // Проверка только для абсолютно идентичных запросов (с того же устройства)
    if (isDuplicateRequest(req.params.token, client.clientId, client.pageSessionId)) {
      return sendJson(res, 200, { ok: true, duplicate: true });
    }

    if (!item.activations) item.activations = [];
    if (!item.violations) item.violations = [];

    // ============= ПОЛУЧАЕМ АКТУАЛЬНЫЕ ДАННЫЕ ГРУППЫ =============
    const groupLinks = db.data.links.filter(x => x.subscriptionUrl === item.subscriptionUrl);
    
    let allActivations = [];
    let allViolations = [];
    for (const link of groupLinks) {
      allActivations.push(...(link.activations || []));
      allViolations.push(...(link.violations || []));
    }
    
    allActivations.sort((a, b) => new Date(a.at) - new Date(b.at));
    
    const groupHasActivations = allActivations.length > 0;
    const groupPrimaryDeviceKey = allActivations[0]?.deviceKey || null;
    
    const totalUsed = allActivations.length + allViolations.length;
    const totalGroupLimit = groupLinks.reduce((sum, link) => sum + (link.maxActivations || 0), 0);
    // ==============================================================

    // Проверяем лимит (ВСЕ использования)
    if (totalUsed >= totalGroupLimit) {
      for (const link of groupLinks) {
        link.status = 'used';
      }
      await saveDb();
      return sendJson(res, 410, { ok: false, error: 'Лимит активаций группы исчерпан' });
    }

    // Если в группе уже есть активации
    if (groupHasActivations) {
      // Проверяем, совпадает ли устройство с первым
      if (deviceKey === groupPrimaryDeviceKey) {
        // То же устройство - обычная активация
        item.activations.push({
          at: now,
          ...client,
          deviceKey,
          rawDeviceKey
        });
      } else {
        // Другое устройство - нарушитель
        item.violations.push({
          at: now,
          ...client,
          deviceKey,
          rawDeviceKey,
          reason: 'different-device'
        });
        
        item.status = 'violator';
        item.lastUsedAt = now;
        
        await saveDb();
        
        const remaining = Math.max(totalGroupLimit - (totalUsed + 1), 0);
        
        return sendJson(res, 403, {
          ok: false,
          error: 'Эта подписка уже активирована на другом устройстве (активация списана)',
          boundDevice: buildUserFacingDeviceInfo(allActivations[0]),
          remaining: remaining
        });
      }
    } else {
      // Первая активация
      item.activations.push({
        at: now,
        ...client,
        deviceKey,
        rawDeviceKey
      });
    }

    // Обновляем счетчики
    item.lastUsedAt = now;
    
    const newTotalUsed = allActivations.length + allViolations.length + 1;
    item.status = newTotalUsed >= totalGroupLimit ? 'used' : (item.violations.length > 0 ? 'violator' : 'active');

    await saveDb();

    return sendJson(res, 200, {
      ok: true,
      remaining: Math.max(totalGroupLimit - newTotalUsed, 0)
    });
  });
});

app.get('/api/admin/links', requireAdmin, async (req, res) => {
  await db.read();
  const items = db.data.links.map(mapLinkForAdmin);
  return sendJson(res, 200, { ok: true, items });
});

app.get('/api/admin/groups', requireAdmin, async (req, res) => {
  await db.read();
  const groups = buildGroups(db.data.links || []);
  return sendJson(res, 200, { ok: true, groups });
});

app.delete('/api/admin/link/:id', requireAdmin, async (req, res) => {
  await db.read();

  const before = db.data.links.length;
  db.data.links = db.data.links.filter(x => x.id !== req.params.id);

  if (db.data.links.length === before) {
    return sendJson(res, 404, { ok: false, error: 'Ссылка не найдена' });
  }

  await saveDb();
  return sendJson(res, 200, { ok: true });
});

// QR код генерация - ПРОСТАЯ ВЕРСИЯ
app.get('/api/qrcode/:token', async (req, res) => {
  try {
    // Проверяем существование токена
    await db.read();
    const item = db.data.links.find(x => x.token === req.params.token);
    
    if (!item) {
      return res.status(404).json({ ok: false, error: 'Токен не найден' });
    }
    
    const baseUrl = `${req.protocol}://${req.get('host')}`;
    const fullUrl = `${baseUrl}/r/${req.params.token}`;
    
    console.log('Generating QR for:', fullUrl);
    
    // Генерируем QR код
    const qrBuffer = await QRCode.toBuffer(fullUrl, {
      type: 'png',
      width: 300,
      margin: 2,
      errorCorrectionLevel: 'H',
      color: {
        dark: '#000000',
        light: '#FFFFFF'
      }
    });
    
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.send(qrBuffer);
  } catch (error) {
    console.error('QR generation error:', error);
    res.status(500).json({ ok: false, error: 'Ошибка генерации QR кода: ' + error.message });
  }
});

// Также добавим endpoint с префиксом /happ для совместимости
app.get('/happ/api/qrcode/:token', async (req, res) => {
  try {
    await db.read();
    const item = db.data.links.find(x => x.token === req.params.token);
    
    if (!item) {
      return res.status(404).json({ ok: false, error: 'Токен не найден' });
    }
    
    const baseUrl = `${req.protocol}://${req.get('host')}`;
    const fullUrl = `${baseUrl}/happ/r/${req.params.token}`;
    
    console.log('Generating QR for (with /happ):', fullUrl);
    
    const qrBuffer = await QRCode.toBuffer(fullUrl, {
      type: 'png',
      width: 300,
      margin: 2,
      errorCorrectionLevel: 'H',
      color: {
        dark: '#000000',
        light: '#FFFFFF'
      }
    });
    
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.send(qrBuffer);
  } catch (error) {
    console.error('QR generation error:', error);
    res.status(500).json({ ok: false, error: 'Ошибка генерации QR кода: ' + error.message });
  }
});

app.delete('/api/admin/link/:id/activations/:index', requireAdmin, async (req, res) => {
  await db.read();

  const item = db.data.links.find(x => x.id === req.params.id);

  if (!item) {
    return sendJson(res, 404, { ok: false, error: 'Ссылка не найдена' });
  }

  const index = Number(req.params.index);

  if (!Number.isInteger(index) || index < 0 || index >= (item.activations?.length || 0)) {
    return sendJson(res, 400, { ok: false, error: 'Активация не найдена' });
  }

  item.activations.splice(index, 1);

  const totalUsed = (item.activations?.length || 0) + (item.violations?.length || 0);
  item.lastUsedAt = item.activations?.length
    ? item.activations[item.activations.length - 1].at
    : (item.violations?.length ? item.violations[item.violations.length - 1].at : null);

  if ((item.violations?.length || 0) > 0) {
    item.status = 'violator';
  } else if (totalUsed >= item.maxActivations) {
    item.status = 'used';
  } else {
    item.status = 'active';
  }

  await saveDb();
  return sendJson(res, 200, { ok: true });
});

app.delete('/api/admin/link/:id/violations/:index', requireAdmin, async (req, res) => {
  await db.read();

  const item = db.data.links.find(x => x.id === req.params.id);

  if (!item) {
    return sendJson(res, 404, { ok: false, error: 'Ссылка не найдена' });
  }

  const index = Number(req.params.index);

  if (!Number.isInteger(index) || index < 0 || index >= (item.violations?.length || 0)) {
    return sendJson(res, 400, { ok: false, error: 'Нарушение не найдено' });
  }

  item.violations.splice(index, 1);

  const totalUsed = (item.activations?.length || 0) + (item.violations?.length || 0);

  if ((item.violations?.length || 0) > 0) {
    item.status = 'violator';
  } else if (totalUsed >= item.maxActivations) {
    item.status = 'used';
  } else {
    item.status = 'active';
  }

  item.lastUsedAt = item.activations?.length
    ? item.activations[item.activations.length - 1].at
    : (item.violations?.length ? item.violations[item.violations.length - 1].at : null);

  await saveDb();
  return sendJson(res, 200, { ok: true });
});

app.delete('/api/admin/link/:id/reset', requireAdmin, async (req, res) => {
  await db.read();

  const item = db.data.links.find(x => x.id === req.params.id);

  if (!item) {
    return sendJson(res, 404, { ok: false, error: 'Ссылка не найдена' });
  }

  item.activations = [];
  item.violations = [];
  item.usedCount = 0;
  item.lastUsedAt = null;
  item.status = 'active';

  await saveDb();
  return sendJson(res, 200, { ok: true });
});

app.get('/r/:token', async (req, res) => {
  await db.read();

  const item = db.data.links.find(x => x.token === req.params.token);

  if (!item) {
    return res.sendFile(path.join(__dirname, 'public', 'invalid.html'));
  }

  const activations = item.activations || [];
  const violations = item.violations || [];
  const totalUsed = activations.length + violations.length;

  if (totalUsed >= item.maxActivations) {
    return res.sendFile(path.join(__dirname, 'public', 'used.html'));
  }

  return res.sendFile(path.join(__dirname, 'public', 'redeem.html'));
});

app.use((req, res) => {
  return sendJson(res, 404, {
    ok: false,
    error: `Маршрут не найден: ${req.method} ${req.originalUrl}`
  });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server started on port ${PORT}`);
  console.log(`Admin URL: http://localhost:${PORT}/admin.html`);
  console.log(`Admin user: ${ADMIN_USER}`);
  console.log(`Admin pass: ${ADMIN_PASS}`);
});
