/**
 * 👑 A QUEDA DO REI — v3.1
 * Deploy: Render Free · Persistência: Upstash · TTS: node-gtts
 */

require('dotenv').config();
const express   = require('express');
const http      = require('http');
const path      = require('path');
const fs        = require('fs');
const crypto    = require('crypto');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const Redis     = require('ioredis');
const gTTS      = require('node-gtts');
const rateLimit = require('express-rate-limit');

const PORT             = Number(process.env.PORT || 3000);
const TIKTOK_USERNAME  = process.env.TIKTOK_USERNAME || '';
const REDIS_URL        = process.env.REDIS_URL || '';
const TWINS_MODE       = String(process.env.TWINS_MODE).toLowerCase() === 'true';
const TTS_ENABLED      = String(process.env.TTS_ENABLED ?? 'true').toLowerCase() === 'true';
const TTS_MIN_DIAMONDS = Number(process.env.TTS_MIN_DIAMONDS || 5);
const ELECTION_ENABLED = String(process.env.ELECTION_ENABLED ?? 'true').toLowerCase() === 'true';

const BASE_HP                   = 100000;
const HP_MULTIPLIER_PER_KINGDOM = 1.8;
const LIKE_BASE_DMG     = 15;
const CHAT_BASE_DMG     = 60;
const CHAT_KEYWORD      = /\batacar\b/i;
const CHAT_KEYWORD_MULT = 2;
const CRIT_CHANCE = 0.05;
const CRIT_MULT   = 3;
const COMBO_TIMEOUT_MS = 2500;
const VICTORY_RESET_MS = 8000;
const ELECTION_MS      = VICTORY_RESET_MS - 800;
const GIFT_DIAMOND_DMG = 150;
const GIFT_MAP = {
  'Rosa': 1000, 'Rose': 1000,
  'Milho': 5000, 'Corn': 5000,
  'Leão': 'PERCENT_50', 'Lion': 'PERCENT_50', 'TikTok': 'PERCENT_50',
};

if (!TIKTOK_USERNAME) {
  console.error('❌ Defina TIKTOK_USERNAME no .env');
  process.exit(1);
}

const logBuffer = [];
const originalLog = console.log;
const originalErr = console.error;
console.log = (...args) => {
  logBuffer.push({ t: new Date().toISOString(), level: 'info', msg: args.join(' ') });
  if (logBuffer.length > 200) logBuffer.shift();
  originalLog(...args);
};
console.error = (...args) => {
  logBuffer.push({ t: new Date().toISOString(), level: 'error', msg: args.join(' ') });
  if (logBuffer.length > 200) logBuffer.shift();
  originalErr(...args);
};

let redis = null;
if (REDIS_URL) {
  redis = new Redis(REDIS_URL, {
    lazyConnect: false,
    maxRetriesPerRequest: 3,
    retryStrategy: (t) => Math.min(t * 300, 5000),
    connectTimeout: 10000,
  });
  redis.on('error', (e) => console.error('Redis:', e.message));
  redis.on('connect', () => console.log('💾 Redis conectado'));
}

const RK = { kingdoms: 'qdR:kingdoms', rankings: 'qdR:rankings' };

async function persistAll() {
  if (!redis) return;
  try {
    const snapshot = kingdoms.map(k => ({
      id: k.id, kingdom: k.kingdom, maxHp: k.maxHp,
      currentHp: k.currentHp, furia: k.furia, running: k.running,
      chosenOne: k.chosenOne || null,
    }));
    const rankings = {};
    for (const k of kingdoms) rankings[k.id] = [...k.ranking.values()];
    await redis.pipeline()
      .set(RK.kingdoms, JSON.stringify(snapshot))
      .set(RK.rankings, JSON.stringify(rankings))
      .exec();
  } catch (e) { console.error('persistAll:', e.message); }
}

async function loadAll() {
  if (!redis) return null;
  try {
    const [snap, rks] = await Promise.all([redis.get(RK.kingdoms), redis.get(RK.rankings)]);
    if (!snap) return null;
    return { kingdoms: JSON.parse(snap), rankings: rks ? JSON.parse(rks) : {} };
  } catch { return null; }
}

function createKingdom(id, kingdom = 1, maxHp = BASE_HP) {
  return {
    id, kingdom, maxHp, currentHp: maxHp,
    furia: false, running: true,
    ranking: new Map(), combos: new Map(),
    finalBlow: null, chosenOne: null,
  };
}

let kingdoms = [];

function publicKingdom(k) {
  return {
    id: k.id, kingdom: k.kingdom, maxHp: k.maxHp,
    currentHp: k.currentHp, furia: k.furia, running: k.running,
    chosenOne: k.chosenOne || null,
    ranking: [...k.ranking.values()].sort((a, b) => b.damage - a.damage).slice(0, 5),
  };
}
const publicState = () => kingdoms.map(publicKingdom);

function kingdomForUser(userId) {
  if (!TWINS_MODE || kingdoms.length === 1) return kingdoms[0];
  const hash = crypto.createHash('md5').update(String(userId)).digest()[0];
  return kingdoms[hash % kingdoms.length];
}

const comboMultiplier = (likes) =>
  likes >= 100 ? 4 : likes >= 50 ? 3 : likes >= 20 ? 2 : 1;

const rollCrit = () => Math.random() < CRIT_CHANCE;

function matchesChosen(username, chosen) {
  if (!chosen || !username) return false;
  const u = String(username).toLowerCase().trim();
  const c = String(chosen).toLowerCase().trim();
  return u === c || u.startsWith(c + ' ');
}

function addDamage(user, rawDamage, meta = {}) {
  const k = meta.kingdom || kingdomForUser(user.userId);
  if (!k.running || k.currentHp <= 0) return;

  const isChosen = matchesChosen(user.username, k.chosenOne);
  const chosenMult = isChosen ? 2 : 1;
  const isCrit = meta.crit ?? rollCrit();
  const damage = Math.max(1, Math.floor(
    rawDamage * chosenMult * (isCrit ? CRIT_MULT : 1)
  ));

  const entry = k.ranking.get(user.userId) || {
    userId: user.userId, username: user.username, avatar: user.avatar, damage: 0,
  };
  entry.damage += damage;
  entry.username = user.username;
  entry.avatar = user.avatar;
  k.ranking.set(user.userId, entry);

  const before = k.currentHp;
  k.currentHp = Math.max(0, k.currentHp - damage);
  const dealt = before - k.currentHp;

  const isFuria = k.currentHp / k.maxHp < 0.3;
  if (isFuria !== k.furia) {
    k.furia = isFuria;
    io.emit('furia', { kingdomId: k.id, furia: isFuria });
  }

  io.emit('damage', {
    kingdomId: k.id,
    userId: user.userId,
    username: user.username,
    avatar: user.avatar,
    amount: dealt,
    crit: isCrit,
    chosen: isChosen,
    type: meta.type || 'chat',
    combo: meta.combo || 1,
    giftName: meta.giftName || null,
  });

  dirty = true;

  if (k.currentHp <= 0) triggerVictory(k, user);
  else io.emit('state', publicState());
}

function triggerVictory(k, user) {
  k.running = false;
  k.finalBlow = { userId: user.userId, username: user.username, avatar: user.avatar };
  io.emit('victory', { ...k.finalBlow, kingdom: k.kingdom, kingdomId: k.id });

  if (ELECTION_ENABLED && k === kingdoms[0] && !election.active) {
    startElection(ELECTION_MS);
  }
  setTimeout(() => resetKingdom(k), VICTORY_RESET_MS);
}

function resetKingdom(k) {
  k.kingdom += 1;
  k.maxHp = Math.floor(k.maxHp * HP_MULTIPLIER_PER_KINGDOM);
  k.currentHp = k.maxHp;
  k.furia = false;
  k.ranking.clear();
  k.running = true;
  k.finalBlow = null;
  for (const c of k.combos.values()) if (c.timer) clearTimeout(c.timer);
  k.combos.clear();
  dirty = true;
  persistAll().catch(() => {});
  io.emit('reset', publicState());
  console.log(`⚔️  Reino ${k.id} → Reinado ${k.kingdom} | HP ${k.maxHp} | Alvo: ${k.chosenOne || '—'}`);
}

const election = {
  active: false,
  votes: new Map(),
  endsAt: 0,
  winner: null,
  timer: null,
};

function startElection(durationMs) {
  election.active = true;
  election.votes.clear();
  election.winner = null;
  election.endsAt = Date.now() + durationMs;
  io.emit('election-start', { endsAt: election.endsAt, durationMs });
  console.log(`🗳️  Eleição iniciada (${durationMs / 1000}s)`);
  if (election.timer) clearTimeout(election.timer);
  election.timer = setTimeout(finishElection, durationMs);
}

function castVote(voterId, voterName, targetName) {
  if (!election.active) return;
  if (election.votes.has(voterId)) return;
  const clean = String(targetName).replace(/[^\p{L}\p{N}_ .-]/gu, '').trim().slice(0, 20);
  if (clean.length < 2) return;
  election.votes.set(voterId, { target: clean, voterName });
  io.emit('election-vote', { voterName, target: clean, total: election.votes.size });
}

function finishElection() {
  election.active = false;
  const tally = new Map();
  for (const v of election.votes.values()) {
    const key = v.target.toLowerCase();
    const cur = tally.get(key) || { name: v.target, count: 0 };
    cur.count += 1;
    tally.set(key, cur);
  }
  let winner = null, max = 0;
  for (const v of tally.values()) if (v.count > max) { max = v.count; winner = v.name; }
  election.winner = winner;

  io.emit('election-end', { winner, votes: max, total: election.votes.size });
  console.log(`🗳️  Eleição encerrada | Vencedor: ${winner || '—'} (${max} votos)`);

  for (const k of kingdoms) k.chosenOne = winner;
  dirty = true;
  persistAll().catch(() => {});
}

const TTS_DIR = path.join(__dirname, 'public', 'tts');
fs.mkdirSync(TTS_DIR, { recursive: true });
for (const f of fs.readdirSync(TTS_DIR)) {
  if (f.endsWith('.mp3')) fs.unlink(path.join(TTS_DIR, f), () => {});
}

const ttsQueue = [];
let ttsBusy = false;
const ttsPT = gTTS('pt-br');

function enqueueTTS(text) {
  if (!TTS_ENABLED) return;
  ttsQueue.push(text);
  processTTS();
}

function processTTS() {
  if (ttsBusy || ttsQueue.length === 0) return;
  ttsBusy = true;
  const text = ttsQueue.shift();
  const file = path.join(TTS_DIR, `tts_${Date.now()}_${Math.random().toString(36).slice(2, 7)}.mp3`);

  const timeout = setTimeout(() => {
    console.warn('TTS timeout — pulando item');
    ttsBusy = false;
    processTTS();
  }, 8000);

  ttsPT.save(file, text, (err) => {
    clearTimeout(timeout);
    ttsBusy = false;
    if (err) {
      console.error('TTS erro:', err.message);
      return processTTS();
    }
    io.emit('tts', { url: `/tts/${path.basename(file)}`, text });
    setTimeout(() => fs.unlink(file, () => {}), 60000);
    setTimeout(processTTS, 800);
  });
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  pingTimeout: 20000,
  pingInterval: 25000,
});

const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Muitas requisições. Aguarde.' },
});

app.set('trust proxy', 1);

app.get('/health', limiter, (req, res) => {
  res.status(200).json({
    status: 'ok',
    uptime: Math.floor(process.uptime()),
    timestamp: new Date().toISOString(),
    kingdoms: kingdoms.map(k => ({
      id: k.id, kingdom: k.kingdom,
      hp: `${Math.floor(k.currentHp)}/${k.maxHp}`,
      running: k.running,
    })),
    tiktokConnected,
    electionActive: election.active,
  });
});

app.get('/logs', limiter, (req, res) => res.json(logBuffer));

app.use(express.static(path.join(__dirname, 'public')));

const socketConnections = new Map();

io.on('connection', (socket) => {
  const ip = (socket.handshake.headers['x-forwarded-for'] || '')
    .split(',')[0].trim() || socket.handshake.address;

  const count = (socketConnections.get(ip) || 0) + 1;
  socketConnections.set(ip, count);

  if (count > 8) {
    console.warn(`🚫 Limite de conexões excedido: ${ip} (${count})`);
    socket.emit('error-msg', { message: 'Muitas conexões deste IP' });
    socket.disconnect(true);
    return;
  }

  socket.on('disconnect', () => {
    const c = socketConnections.get(ip) || 1;
    if (c <= 1) socketConnections.delete(ip);
    else socketConnections.set(ip, c - 1);
  });

  socket.emit('state', publicState());
  for (const k of kingdoms) {
    if (k.finalBlow) socket.emit('victory', { ...k.finalBlow, kingdom: k.kingdom, kingdomId: k.id });
  }
  if (election.active) socket.emit('election-start', { endsAt: election.endsAt });
});

let tiktok = null;
let tiktokConnected = false;
let reconnectScheduled = false;

function connectTikTok() {
  if (tiktok) {
    try { tiktok.disconnect(); } catch {}
    try { tiktok.removeAllListeners(); } catch {}
    tiktok = null;
  }
  tiktokConnected = false;

  tiktok = new WebcastPushConnection(TIKTOK_USERNAME, {
    processInitialData: false,
    enableExtendedGiftInfo: true,
  });

  tiktok.connect()
    .then((s) => {
      tiktokConnected = true;
      reconnectScheduled = false;
      console.log(`✅ TikTok Live conectada (roomId: ${s.roomId})`);
    })
    .catch((err) => {
      console.error('❌ Conexão TikTok falhou:', err.message);
      scheduleReconnect();
    });

  tiktok.on('disconnected', () => {
    tiktokConnected = false;
    console.warn('⚠️  Desconectado');
    scheduleReconnect();
  });

  tiktok.on('error', (e) => console.error('TikTok error:', e.message));

  tiktok.on('like', (data) => {
    const k = kingdomForUser(data.userId);
    const batchCount = data.likeCount || 1;

    let c = k.combos.get(data.userId);
    if (!c) { c = { likes: 0, timer: null }; k.combos.set(data.userId, c); }
    c.likes += batchCount;
    if (c.timer) clearTimeout(c.timer);
    c.timer = setTimeout(() => {
      k.combos.delete(data.userId);
      io.emit('combo-reset', { userId: data.userId, kingdomId: k.id });
    }, COMBO_TIMEOUT_MS);

    const mult = comboMultiplier(c.likes);
    addDamage(
      { userId: data.userId, username: data.nickname || data.uniqueId, avatar: data.profilePictureUrl },
      LIKE_BASE_DMG * batchCount * mult,
      { type: 'like', combo: mult, kingdom: k }
    );
  });

  tiktok.on('chat', (data) => {
    const raw = (data.comment || '').trim();

    if (election.active && raw) {
      const m = raw.match(/^!voto\s+([\p{L}\p{N}_ .-]{2,20})/iu);
      if (m) castVote(data.userId, data.nickname || data.uniqueId, m[1].trim());
    }

    const isAttack = CHAT_KEYWORD.test(raw);
    addDamage(
      { userId: data.userId, username: data.nickname || data.uniqueId, avatar: data.profilePictureUrl },
      CHAT_BASE_DMG * (isAttack ? CHAT_KEYWORD_MULT : 1),
      { type: isAttack ? 'chat-attack' : 'chat' }
    );
  });

  tiktok.on('gift', (data) => {
    if (data.giftType === 1 && !data.repeatEnd) return;

    const k = kingdomForUser(data.userId);
    const giftName = data.giftName || 'Presente';
    const diamonds = data.diamondCount || 1;
    const repeat   = data.repeatCount || 1;

    let baseDmg;
    const mapped = GIFT_MAP[giftName];
    if (mapped === 'PERCENT_50') baseDmg = Math.floor(k.maxHp * 0.5);
    else if (typeof mapped === 'number') baseDmg = mapped * repeat;
    else baseDmg = diamonds * GIFT_DIAMOND_DMG * repeat;

    io.emit('gift-event', {
      kingdomId: k.id,
      userId: data.userId,
      username: data.nickname || data.uniqueId,
      giftName, giftImage: data.giftPictureUrl,
      diamonds, repeat, damage: baseDmg,
    });

    if (diamonds >= TTS_MIN_DIAMONDS) {
      const name = data.nickname || data.uniqueId;
      const team = TWINS_MODE ? ` no Reino ${k.id}` : '';
      enqueueTTS(`${name} enviou ${giftName}! Causou ${baseDmg} de dano ao Rei Tirano${team}.`);
    }

    addDamage(
      { userId: data.userId, username: data.nickname || data.uniqueId, avatar: data.profilePictureUrl },
      baseDmg,
      { type: 'gift', giftName, kingdom: k }
    );
  });
}

function scheduleReconnect() {
  if (reconnectScheduled) return;
  reconnectScheduled = true;
  setTimeout(() => {
    reconnectScheduled = false;
    connectTikTok();
  }, 5000);
}

let dirty = false;
setInterval(() => { if (dirty) { dirty = false; persistAll(); } }, 15000);

(async () => {
  const saved = await loadAll();
  if (saved?.kingdoms?.length) {
    kingdoms = saved.kingdoms.map(k => {
      const inst = createKingdom(k.id, k.kingdom, k.maxHp);
      inst.currentHp = k.currentHp;
      inst.furia = k.furia;
      inst.running = k.running;
      inst.chosenOne = k.chosenOne || null;
      const rk = saved.rankings?.[k.id] || [];
      for (const p of rk) inst.ranking.set(p.userId, p);
      return inst;
    });
    console.log(`💾 Estado restaurado: ${kingdoms.length} reino(s)`);
  } else {
    kingdoms = [createKingdom('A')];
    if (TWINS_MODE) kingdoms.push(createKingdom('B'));
    console.log(`🆕 Novo jogo: ${kingdoms.length} reino(s)`);
  }

  connectTikTok();

  server.listen(PORT, () => {
    console.log(`🚀 http://localhost:${PORT}`);
    console.log(`🎯 @${TIKTOK_USERNAME} | Twins: ${TWINS_MODE} | TTS: ${TTS_ENABLED} | Eleição: ${ELECTION_ENABLED}`);
  });
})();

async function shutdown(signal) {
  console.log(`\n🛑 ${signal} recebido — encerrando...`);
  try { await persistAll(); } catch {}
  if (tiktok) try { tiktok.disconnect(); } catch {}
  if (redis)  try { redis.disconnect(); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000);
}
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException',  (e) => console.error('Uncaught:', e.stack || e));
process.on('unhandledRejection', (e) => console.error('Unhandled:', e));
