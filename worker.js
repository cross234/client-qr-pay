// ============================================================
//  Client QR Pay — Cloudflare Worker
//  White-label QR payment app with hidden P2P proxy
// ============================================================

// ── tiny helpers ──────────────────────────────────────────────
const json = (d, s = 200) => new Response(JSON.stringify(d), {
  status: s, headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "*", "Access-Control-Allow-Methods": "*" }
});
const bad = (m, s = 400) => json({ ok: false, error: m }, s);
const now = () => Date.now();
const uid = () => crypto.randomUUID().replace(/-/g, "").slice(0, 16);
const rnd6 = () => String(Math.floor(100000 + Math.random() * 900000));

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "*",
    "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,OPTIONS",
  };
}
function corsRes() {
  return new Response(null, { status: 204, headers: corsHeaders() });
}

// ── KV helpers ───────────────────────────────────────────────
async function kvGet(env, key) {
  const v = await env.KV.get(key, "text");
  if (!v) return null;
  try { return JSON.parse(v); } catch { return v; }
}
async function kvPut(env, key, val, opts) {
  await env.KV.put(key, typeof val === "string" ? val : JSON.stringify(val), opts || {});
}
async function kvDel(env, key) {
  await env.KV.delete(key);
}
async function kvList(env, prefix, limit = 1000) {
  const out = [];
  let cursor;
  while (true) {
    const r = await env.KV.list({ prefix, limit: Math.min(limit - out.length, 1000), cursor });
    for (const k of r.keys) out.push(k.name);
    if (r.list_complete || out.length >= limit) break;
    cursor = r.cursor;
  }
  return out;
}

// ── Telegram Bot helpers ─────────────────────────────────────
async function tg(env, method, body) {
  const token = env.BOT_TOKEN || "";
  const r = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return r.json();
}
async function tgSend(env, chatId, text, extra) {
  return tg(env, "sendMessage", { chat_id: chatId, text, parse_mode: "HTML", ...extra });
}
async function tgNotifyAdmins(env, text) {
  const cfg = await getSettings(env);
  const ids = cfg.adminTgIds || [];
  for (const id of ids) {
    try { await tgSend(env, id, text); } catch {}
  }
}

// ── Settings ─────────────────────────────────────────────────
const DEFAULT_SETTINGS = {
  adminTgIds: [],
  adminToken: "",
  usdtWallet: "",
  usdtNetwork: "TRC20",
  rateMode: "rapira",       // "rapira" | "manual"
  manualRate: 0,
  rateMarkupPercent: 0,      // наценка к курсу в процентах (+1.5 = +1.5%)
  botUsername: "",
  siteName: "QR Pay",
  mainWorkerUrl: "",         // URL основного обменника (для P2P прокси)
  mainAdminToken: "",        // Токен для основного обменника
};

async function getSettings(env) {
  const s = await kvGet(env, "cfg");
  return Object.assign({}, DEFAULT_SETTINGS, s || {});
}
async function saveSettings(env, patch) {
  const cur = await getSettings(env);
  const merged = Object.assign(cur, patch);
  await kvPut(env, "cfg", merged);
  return merged;
}

// ── Rate ─────────────────────────────────────────────────────
let _rateCache = { rate: 0, ts: 0 };

// Fetch with a hard timeout so slow external APIs never block /api/me
function fetchWithTimeout(url, opts, ms) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  return fetch(url, { ...opts, signal: ctrl.signal }).finally(() => clearTimeout(t));
}

async function fetchRapiraRate() {
  // Primary: crossflag (3 s timeout — it has its own KV lastRates fallback)
  try {
    const r = await fetchWithTimeout(
      "https://api.crossflag.org/api/public/rapira_rate",
      { headers: { Accept: "application/json" } },
      3000
    );
    const j = await r.json();
    if (j.ok && j.rate > 0) return Number(j.rate);
  } catch {}

  // Fallback: Rapira direct (2 s timeout)
  try {
    const r = await fetchWithTimeout(
      "https://api.rapira.net/market/overview",
      { headers: { "User-Agent": "Mozilla/5.0" } },
      2000
    );
    const j = await r.json();
    const allPairs = [
      ...(j.changeRank || []),
      ...(j.changeRankDown || []),
      ...(j.recommend || []),
    ];
    const usdt = allPairs.find(p => p.symbol === "USDT/RUB");
    if (usdt && usdt.close > 0) return Number(usdt.close);
  } catch {}

  // Fallback: Garantex (2 s timeout)
  try {
    const r = await fetchWithTimeout(
      "https://garantex.org/api/v2/trades?market=usdtrub&limit=1",
      {},
      2000
    );
    const j = await r.json();
    if (Array.isArray(j) && j.length > 0 && j[0].price) return Number(j[0].price);
  } catch {}

  return 0;
}

function applyMarkup(baseRate, pct) {
  if (!pct) return baseRate;
  return baseRate * (1 + Number(pct) / 100);
}

async function getRate(env) {
  const cfg = await getSettings(env);
  const pct = cfg.rateMarkupPercent || 0;
  if (cfg.rateMode === "manual" && cfg.manualRate > 0) {
    return applyMarkup(cfg.manualRate, pct);
  }
  // In-memory cache (valid 60 s, survives within same isolate)
  if (_rateCache.rate > 0 && now() - _rateCache.ts < 60000) {
    return applyMarkup(_rateCache.rate, pct);
  }
  // KV-persisted cache (valid 10 min, survives cold starts)
  try {
    const cached = await kvGet(env, "mw:rate_cache");
    if (cached?.rate > 0 && now() - (cached.ts || 0) < 600000) {
      _rateCache = { rate: cached.rate, ts: cached.ts };
      return applyMarkup(cached.rate, pct);
    }
  } catch {}
  // Fetch live rate (total budget: ~7 s across all sources)
  const rate = await fetchRapiraRate();
  if (rate > 0) {
    _rateCache = { rate, ts: now() };
    // Save to KV so next cold start gets it instantly
    kvPut(env, "mw:rate_cache", { rate, ts: now() }, { expirationTtl: 3600 }).catch(() => {});
    return applyMarkup(rate, pct);
  }
  // Last resort: stale in-memory value
  return _rateCache.rate > 0 ? applyMarkup(_rateCache.rate, pct) : 0;
}

// ── TG WebApp initData validation ────────────────────────────
async function validateTgInitData(initData, botToken) {
  const params = new URLSearchParams(initData);
  const hash = params.get('hash');
  params.delete('hash');
  const entries = [...params.entries()].sort((a,b) => a[0].localeCompare(b[0]));
  const dataCheckString = entries.map(([k,v]) => k + '=' + v).join('\n');

  const encoder = new TextEncoder();
  const secretKey = await crypto.subtle.importKey('raw', encoder.encode('WebAppData'), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const secret = await crypto.subtle.sign('HMAC', secretKey, encoder.encode(botToken));
  const key = await crypto.subtle.importKey('raw', secret, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await crypto.subtle.sign('HMAC', key, encoder.encode(dataCheckString));

  const hexHash = [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('');
  return hexHash === hash;
}

// ── Auth helpers ─────────────────────────────────────────────
async function getUser(env, tgId) {
  return kvGet(env, `u:${tgId}`);
}
async function saveUser(env, user) {
  await kvPut(env, `u:${user.tgId}`, user);
  if (user.username) {
    await kvPut(env, `umap:${user.username.toLowerCase()}`, String(user.tgId));
  }
}
async function getUserByUsername(env, username) {
  const clean = String(username || "").replace(/^@/, "").toLowerCase().trim();
  if (!clean) return null;
  const tgId = await kvGet(env, `umap:${clean}`);
  if (!tgId) return null;
  return getUser(env, tgId);
}
async function getUserByToken(env, token) {
  if (!token) return null;
  const tgId = await kvGet(env, `sess:${token}`);
  if (!tgId) return null;
  return getUser(env, tgId);
}
function authFromReq(req) {
  return (req.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "").trim() ||
    new URL(req.url).searchParams.get("token") || "";
}
function adminTokenFromReq(req) {
  return req.headers.get("X-Admin-Token") || "";
}
async function requireUser(req, env) {
  const token = authFromReq(req);
  if (!token) return [null, bad("Unauthorized", 401)];
  const user = await getUserByToken(env, token);
  if (!user) return [null, bad("Invalid token", 401)];
  return [user, null];
}
async function requireAdmin(req, env) {
  const cfg = await getSettings(env);
  const tok = adminTokenFromReq(req);
  if (!tok || tok !== cfg.adminToken) return [null, bad("Forbidden", 403)];
  return [cfg, null];
}

// ── Balance helpers ──────────────────────────────────────────
// Balance stored as integer (micro-USDT, × 1e6)
function usdtToMicro(usdt) { return Math.round(Number(usdt) * 1e6); }
function microToUsdt(m) { return Number(m) / 1e6; }

async function getBalance(env, tgId) {
  const u = await getUser(env, tgId);
  return Number(u?.balanceMicro || 0);
}
async function adjustBalance(env, tgId, deltaMicro, reason) {
  const u = await getUser(env, tgId);
  if (!u) throw new Error("User not found");
  const prev = Number(u.balanceMicro || 0);
  const next = prev + deltaMicro;
  if (next < 0) throw new Error("Insufficient balance");
  u.balanceMicro = next;
  u.updatedAt = now();
  await saveUser(env, u);
  // log
  const logId = uid();
  await kvPut(env, `blog:${tgId}:${logId}`, {
    id: logId, tgId, prev, delta: deltaMicro, next, reason, ts: now()
  });
  return next;
}

// ════════════════════════════════════════════════════════════
//  ROUTE HANDLERS
// ════════════════════════════════════════════════════════════

function escHtml(s) {
  return String(s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ── MW-native P2P offers ──────────────────────────────────────
async function getMwOffers(env) {
  const o = await kvGet(env, "mwOffers");
  if (!Array.isArray(o)) return [];
  const n = now();
  let changed = false;
  const cleaned = o.map(offer => {
    if (String(offer.status || "").toUpperCase() === "RESERVED" && offer.expiresAt && n > offer.expiresAt) {
      changed = true;
      return { ...offer, status: "NEW", reservedBy: null, reserveId: null, reservedAt: null, expiresAt: null };
    }
    return offer;
  });
  if (changed) kvPut(env, "mwOffers", cleaned).catch(() => {});
  return cleaned;
}
async function saveMwOffers(env, offers) {
  await kvPut(env, "mwOffers", offers);
}
function parseMwAdminReply(text) {
  // Format: <amountRub> <rate> <requisite> <bank...>
  // e.g. "25000 90.5 79001234567 Сбербанк"
  const parts = String(text || "").trim().split(/\s+/);
  if (parts.length < 4) return null;
  const amountRub = Math.floor(Number(String(parts[0]).replace(/[^\d.]/g, "")));
  const rate = Number(String(parts[1]).replace(",", "."));
  const payRequisite = String(parts[2]);
  const payBank = parts.slice(3).join(" ").trim();
  if (!amountRub || amountRub < 100) return null;
  if (!rate || rate < 1 || rate > 9999) return null;
  if (!payRequisite) return null;
  if (!payBank) return null;
  return { amountRub, rate, method: "SBP", payRequisite, payBank };
}
async function completeMwDeal(env, offerId, tgId, amountMicro) {
  const doneKey = `mwdeal:done:${offerId}`;
  if (await kvGet(env, doneKey)) return { ok: true, skipped: true };
  await adjustBalance(env, tgId, amountMicro, `P2P buy: ${offerId}`);
  await kvPut(env, doneKey, { ts: now(), tgId, offerId, amountMicro }, { expirationTtl: 3600 * 24 * 180 });
  const dealKey = `exch:${tgId}:${offerId}`;
  const deal = (await kvGet(env, dealKey)) || {};
  deal.status = "SUCCESS";
  deal.completedAt = now();
  deal.creditedMicro = amountMicro;
  deal.creditedUsdt = (amountMicro / 1e6).toFixed(2);
  await kvPut(env, dealKey, deal);

  // Bookkeeping: update chat debt
  const usdtCredited = amountMicro / 1e6;
  const fromChatId = deal.createdFromChatId;
  if (fromChatId) {
    try {
      const debtKey = `mw_chat_debt:${fromChatId}`;
      const prevDebt = Number((await kvGet(env, debtKey)) || 0);
      const newDebt = prevDebt + usdtCredited;
      await kvPut(env, debtKey, newDebt);
      const shortId = String(offerId).slice(-8);
      await tgSend(env, fromChatId,
        `✅ <b>Сделка BUY ${shortId} завершена успешно</b>\n\n` +
        `💰 Долг по чату: <b>${newDebt.toFixed(2)} USDT</b>\n` +
        `(+${usdtCredited.toFixed(2)} USDT за сделку ${shortId})`,
        { reply_markup: { inline_keyboard: [[
          { text: "💰 Обнулить долг", callback_data: `mwbal:reset:${fromChatId}` },
        ]] } }
      ).catch(() => {});
    } catch {}
  }

  try {
    const idxKey2 = `buyreq_idx:${tgId}`;
    const idxArr = (await kvGet(env, idxKey2)) || [];
    for (const reqId of idxArr.slice(0, 30)) {
      const rec = await kvGet(env, `buyreq:${tgId}:${reqId}`);
      if (rec && rec.status === "PENDING" && (rec.matchedOfferIds || []).includes(offerId)) {
        rec.status = "COMPLETED"; rec.completedAt = now();
        await kvPut(env, `buyreq:${tgId}:${reqId}`, rec);
        break;
      }
    }
  } catch {}
  const usdtStr = (amountMicro / 1e6).toFixed(2);
  await tgSend(env, tgId,
    `✅ <b>Сделка завершена!</b>\n\n💵 Зачислено: <b>${usdtStr} USDT</b>\n\nСредства поступили на ваш баланс Moon Wallet 💰🎉`
  ).catch(() => {});
  return { ok: true, amountMicro };
}

// ── Bot webhook ──────────────────────────────────────────────
async function handleBotWebhook(req, env) {
  let body;
  try { body = await req.json(); } catch { return json({ ok: true }); }

  // ── callback_query: admin clicking ✅/❌ on proof ────────────
  const cb = body.callback_query;
  if (cb && cb.data) {
    const cbChatId = cb.message?.chat?.id;
    const cbMsgId = cb.message?.message_id;
    const cbId = cb.id;
    await tg(env, "answerCallbackQuery", { callback_query_id: cbId }).catch(() => {});

    // ── mwbal bookkeeping callbacks ──────────────────────────
    const mbal = String(cb.data).match(/^mwbal:(reset|add|sub):(-?\d+)$/);
    if (mbal) {
      const op = mbal[1], balChatId = mbal[2];
      const debtKey = `mw_chat_debt:${balChatId}`;
      if (op === "reset") {
        await kvPut(env, debtKey, 0);
        await tgSend(env, cbChatId, `💰 <b>Долг обнулён</b>\n\nДолг по чату: <b>0.00 USDT</b>`).catch(() => {});
      } else {
        await kvPut(env, `mwbal_state:${balChatId}`, { op, ts: now() }, { expirationTtl: 300 });
        const actionText = op === "add" ? "начисления" : "списания";
        await tgSend(env, cbChatId, `💬 Введите сумму USDT для ${actionText} (ответьте числом):`).catch(() => {});
      }
      return json({ ok: true });
    }

    // ── mwbuy:ok/err callbacks ───────────────────────────────
    const m = String(cb.data).match(/^mwbuy:(ok|err):([^:]+):(\d+)$/);
    if (m) {
      const action = m[1], offerId = m[2], tgIdTarget = m[3];
      if (action === "ok") {
        const deal = await kvGet(env, `exch:${tgIdTarget}:${offerId}`);
        const amountRub = Number(deal?.amountRub || 0);
        const rate = Number(deal?.rate || 0);
        const amountMicro = rate > 0 ? Math.trunc((amountRub / rate) * 1e6) : 0;
        if (amountMicro > 0) {
          await completeMwDeal(env, offerId, tgIdTarget, amountMicro);
          await tg(env, "editMessageReplyMarkup", { chat_id: cbChatId, message_id: cbMsgId, reply_markup: { inline_keyboard: [] } }).catch(() => {});
          await tgSend(env, cbChatId, `✅ Сделка <code>${offerId}</code> завершена. +${(amountMicro / 1e6).toFixed(2)} USDT → пользователь ${tgIdTarget}`).catch(() => {});
        } else {
          await tgSend(env, cbChatId, `❌ Не удалось рассчитать сумму для оффера ${offerId}. Сделка не закрыта.`).catch(() => {});
        }
      } else {
        // Error: cancel deal, release offer, notify user
        const dealKey = `exch:${tgIdTarget}:${offerId}`;
        const deal = await kvGet(env, dealKey);
        const notifyFromChat = deal?.createdFromChatId;
        if (deal) await kvPut(env, dealKey, { ...deal, status: "ERROR", updatedAt: now() });
        const allOffers = await getMwOffers(env);
        const oi = allOffers.findIndex(o => o.id === offerId);
        if (oi >= 0) { allOffers[oi] = { ...allOffers[oi], status: "NEW", reservedBy: null, reserveId: null }; await saveMwOffers(env, allOffers); }
        await tgSend(env, tgIdTarget, `❌ <b>Оплата не подтверждена.</b>\n\nАдминистратор отклонил сделку. Обратитесь в поддержку.`).catch(() => {});
        await tg(env, "editMessageReplyMarkup", { chat_id: cbChatId, message_id: cbMsgId, reply_markup: { inline_keyboard: [] } }).catch(() => {});
        await tgSend(env, cbChatId, `❌ Сделка <code>${offerId}</code> отклонена.`).catch(() => {});
        // Also notify the originating chat if different
        if (notifyFromChat && String(notifyFromChat) !== String(cbChatId)) {
          await tgSend(env, notifyFromChat, `❌ Сделка <code>${offerId}</code> отклонена администратором.`).catch(() => {});
        }
      }
    }
    return json({ ok: true });
  }

  const msg = body.message;
  if (!msg) return json({ ok: true });

  const chatId = msg.chat.id;
  const text = String(msg.text || "").trim();
  const from = msg.from || {};
  const tgId = String(from.id || "");
  const username = String(from.username || "").toLowerCase();

  // ── Admin reply to buy request message → create offer ────────
  const replyTo = msg.reply_to_message;
  if (replyTo && replyTo.message_id && text) {
    const reqData = await kvGet(env, `mwreq_msg:${replyTo.message_id}`);
    if (reqData) {
      const parsed = parseMwAdminReply(text);
      if (!parsed) {
        await tgSend(env, chatId,
          `❌ Неверный формат.\n\nИспользуйте: <code>сумма курс реквизит банк</code>\nПример: <code>25000 90.5 79001234567 Сбербанк</code>`
        ).catch(() => {});
      } else {
        const { amountRub, rate, method, payRequisite, payBank } = parsed;
        const offerId = "mwo_" + uid();
        const newOffer = {
          id: offerId, amountRub, rate, method, payRequisite, payBank,
          status: "NEW", mwOnly: true, mwTgId: String(reqData.tgId),
          reqId: reqData.reqId, adminMsgId: replyTo.message_id, createdAt: now(),
          createdFromChatId: String(chatId),
        };
        const allOffers = await getMwOffers(env);
        allOffers.push(newOffer);
        await saveMwOffers(env, allOffers);
        // Update buy request record
        const buyReqKey = `buyreq:${reqData.tgId}:${reqData.reqId}`;
        const reqRec = await kvGet(env, buyReqKey);
        if (reqRec) {
          reqRec.matchedOffersCount = (reqRec.matchedOffersCount || 0) + 1;
          reqRec.matchedOfferIds = [...(reqRec.matchedOfferIds || []), offerId];
          reqRec.approxRate = rate;
          await kvPut(env, buyReqKey, reqRec);
        }
        // Notify the MW user with deep link to exchange
        const usdtStr = (amountRub / rate).toFixed(2);
        const exchUrl = "https://cross2page.pages.dev/exchange.html";
        await tg(env, "sendMessage", {
          chat_id: String(reqData.tgId),
          text: `🎯 <b>Найдена заявка на покупку USDT!</b>\n\n💰 Сумма: <b>${fmtRubTg(amountRub)} ₽</b>\n📊 Курс: <b>${rate.toFixed(2)} ₽/USDT</b>\n💵 Получите: <b>≈ ${usdtStr} USDT</b>\n\nНажмите кнопку ниже чтобы перейти к сделке ⚡`,
          parse_mode: "HTML",
          reply_markup: { inline_keyboard: [[{ text: "🚀 Перейти к сделке", web_app: { url: exchUrl } }]] },
        }).catch(() => {});
        // Confirm to admin
        await tgSend(env, chatId,
          `✅ Предложение создано!\n💰 ${fmtRubTg(amountRub)} ₽ | ${rate.toFixed(2)} ₽/USDT | ${method}\n💳 ${payRequisite}${payBank ? " (" + payBank + ")" : ""}\n≈ ${usdtStr} USDT\nOffer: <code>${offerId}</code>`
        ).catch(() => {});
      }
      return json({ ok: true });
    }
  }

  if (!text) return json({ ok: true });

  if (text === "/start" || text.startsWith("/start ")) {
    let user = await getUser(env, tgId);
    if (!user) {
      user = { tgId, username, firstName: from.first_name || "", lastName: from.last_name || "", photoUrl: "", balanceMicro: 0, createdAt: now(), updatedAt: now(), banned: false };
      await saveUser(env, user);
    } else {
      let changed = false;
      if (username && user.username !== username) { user.username = username; changed = true; }
      if (from.first_name && user.firstName !== from.first_name) { user.firstName = from.first_name; changed = true; }
      if (from.last_name && user.lastName !== from.last_name) { user.lastName = from.last_name; changed = true; }
      if (changed) { user.updatedAt = now(); await saveUser(env, user); }
    }
    await tg(env, "setChatMenuButton", { chat_id: chatId, menu_button: { type: "web_app", text: "Moon Wallet", web_app: { url: "https://cross234.github.io/client-qr-pay/" } } });
    const webAppUrl = "https://cross234.github.io/client-qr-pay/";
    await tgSend(env, chatId, `🌙 Добро пожаловать в <b>Moon Wallet</b>!\n\nВаш кошелёк готов. Нажмите кнопку ниже или используйте меню для входа.`,
      { reply_markup: { inline_keyboard: [[{ text: "🌙 Открыть Moon Wallet", web_app: { url: webAppUrl } }]] } });
    return json({ ok: true });
  }
  if (text === "/login") {
    const user = await getUser(env, tgId);
    if (!user) { await tgSend(env, chatId, "❌ Аккаунт не найден. Напиши /start для регистрации."); return json({ ok: true }); }
    const code = rnd6();
    await kvPut(env, `auth:${code}`, tgId, { expirationTtl: 300 });
    await tgSend(env, chatId, `🔐 Ваш код для входа: <code>${code}</code>\n\nКод действителен 5 минут.`);
    return json({ ok: true });
  }
  if (text === "/balance" || text === "/bal") {
    const user = await getUser(env, tgId);
    if (!user) { await tgSend(env, chatId, "❌ Аккаунт не найден. /start"); return json({ ok: true }); }
    const bal = microToUsdt(user.balanceMicro || 0);
    await tgSend(env, chatId, `💰 Ваш баланс: <b>${bal.toFixed(2)} USDT</b>`);
    return json({ ok: true });
  }

  // ── /buyamount_on — подключить чат для получения запросов на покупку ──
  if (text === "/buyamount_on") {
    const chats = (await kvGet(env, "mwBuyAmountChats")) || [];
    const chatIdStr = String(chatId);
    if (!chats.includes(chatIdStr)) {
      chats.push(chatIdStr);
      await kvPut(env, "mwBuyAmountChats", chats);
    }
    await tgSend(env, chatId,
      `✅ <b>Чат подключён!</b>\n\nТеперь в этот чат будут поступать запросы на покупку USDT от пользователей Moon Wallet.\n\nОтвечайте на сообщения бота в формате:\n<code>сумма курс реквизит банк</code>\nПример: <code>25000 90.5 79001234567 Сбербанк</code>\n\nДля отключения: /buyamount_off`
    ).catch(() => {});
    return json({ ok: true });
  }

  // ── /buyamount_off — отключить чат ───────────────────────────
  if (text === "/buyamount_off") {
    const chats = (await kvGet(env, "mwBuyAmountChats")) || [];
    const filtered = chats.filter(c => String(c) !== String(chatId));
    await kvPut(env, "mwBuyAmountChats", filtered);
    await tgSend(env, chatId, `🔕 Чат отключён. Запросы на покупку больше не будут поступать сюда.`).catch(() => {});
    return json({ ok: true });
  }

  // ── /bal_mw — бухгалтерия чата ───────────────────────────────
  if (text === "/bal_mw") {
    const debtKey = `mw_chat_debt:${chatId}`;
    const debt = Number((await kvGet(env, debtKey)) || 0);
    await tg(env, "sendMessage", {
      chat_id: chatId,
      text: `💰 <b>Долг по чату: ${debt.toFixed(2)} USDT</b>`,
      parse_mode: "HTML",
      reply_markup: { inline_keyboard: [[
        { text: "Обнулить", callback_data: `mwbal:reset:${chatId}` },
        { text: "+ Начислить", callback_data: `mwbal:add:${chatId}` },
        { text: "- Списать", callback_data: `mwbal:sub:${chatId}` },
      ]] },
    }).catch(() => {});
    return json({ ok: true });
  }

  // ── Handle amount input for pending mwbal state ───────────────
  const balState = await kvGet(env, `mwbal_state:${chatId}`);
  if (balState && (balState.op === "add" || balState.op === "sub")) {
    const amount = parseFloat(String(text).replace(",", "."));
    if (!isNaN(amount) && amount > 0) {
      const debtKey = `mw_chat_debt:${chatId}`;
      const prevDebt = Number((await kvGet(env, debtKey)) || 0);
      const newDebt = balState.op === "add" ? prevDebt + amount : Math.max(0, prevDebt - amount);
      await kvPut(env, debtKey, newDebt);
      await kvPut(env, `mwbal_state:${chatId}`, null, { expirationTtl: 1 });
      const actionWord = balState.op === "add" ? "✅ Начислено" : "✅ Списано";
      await tgSend(env, chatId,
        `${actionWord} <b>${amount.toFixed(2)} USDT</b>\n\n💰 Долг по чату: <b>${newDebt.toFixed(2)} USDT</b>`,
        { reply_markup: { inline_keyboard: [[
          { text: "Обнулить", callback_data: `mwbal:reset:${chatId}` },
          { text: "+ Начислить", callback_data: `mwbal:add:${chatId}` },
          { text: "- Списать", callback_data: `mwbal:sub:${chatId}` },
        ]] } }
      ).catch(() => {});
      return json({ ok: true });
    }
  }

  return json({ ok: true });
}

// ── Auth: request code ───────────────────────────────────────
async function handleRequestCode(req, env) {
  const body = await req.json().catch(() => ({}));
  const username = String(body.username || "").replace(/^@/, "").toLowerCase().trim();
  if (!username) return bad("Username required");

  const user = await getUserByUsername(env, username);
  if (!user) return bad("User not found — start the bot first", 404);

  const code = rnd6();
  await kvPut(env, `auth:${code}`, user.tgId, { expirationTtl: 300 });
  await tgSend(env, user.tgId,
    `🔐 Код для входа: <code>${code}</code>\n\n5 минут на ввод.`
  );
  return json({ ok: true });
}

// ── Auth: verify code ────────────────────────────────────────
async function handleVerifyCode(req, env) {
  const body = await req.json().catch(() => ({}));
  const code = String(body.code || "").trim();
  if (!code) return bad("Code required");

  const tgId = await kvGet(env, `auth:${code}`);
  if (!tgId) return bad("Invalid or expired code", 401);

  await kvDel(env, `auth:${code}`);
  const token = uid() + uid();
  await kvPut(env, `sess:${token}`, String(tgId), { expirationTtl: 86400 * 30 });

  const user = await getUser(env, tgId);
  return json({
    ok: true,
    token,
    user: {
      tgId: user.tgId,
      username: user.username,
      firstName: user.firstName,
      balance: microToUsdt(user.balanceMicro || 0),
    }
  });
}

// ── Auth: Telegram WebApp ────────────────────────────────────
async function handleTelegramAuth(req, env) {
  const body = await req.json().catch(() => ({}));
  const initData = String(body.initData || "");
  if (!initData) return bad("initData required");

  const botToken = env.BOT_TOKEN || "";
  if (!botToken) return bad("Bot token not configured", 500);

  const valid = await validateTgInitData(initData, botToken);
  if (!valid) return bad("Invalid initData", 401);

  // Parse user from initData
  const params = new URLSearchParams(initData);
  const userJson = params.get("user");
  if (!userJson) return bad("No user in initData", 400);

  let tgUser;
  try { tgUser = JSON.parse(userJson); } catch { return bad("Invalid user JSON", 400); }

  const tgId = String(tgUser.id || "");
  if (!tgId) return bad("No user id", 400);

  const username = String(tgUser.username || "").toLowerCase();
  const firstName = tgUser.first_name || "";
  const lastName = tgUser.last_name || "";
  const photoUrl = tgUser.photo_url || "";

  // Auto-create user if not exists
  let user = await getUser(env, tgId);
  if (!user) {
    user = {
      tgId,
      username,
      firstName,
      lastName,
      photoUrl,
      balanceMicro: 0,
      createdAt: now(),
      updatedAt: now(),
      banned: false,
    };
    await saveUser(env, user);
  } else {
    // Update fields from Telegram
    let changed = false;
    if (username && user.username !== username) { user.username = username; changed = true; }
    if (firstName && user.firstName !== firstName) { user.firstName = firstName; changed = true; }
    if (lastName && user.lastName !== lastName) { user.lastName = lastName; changed = true; }
    if (photoUrl && user.photoUrl !== photoUrl) { user.photoUrl = photoUrl; changed = true; }
    if (changed) {
      user.updatedAt = now();
      await saveUser(env, user);
    }
  }

  // Create session token
  const token = uid() + uid();
  await kvPut(env, `sess:${token}`, String(tgId), { expirationTtl: 86400 * 30 });

  const rate = await getRate(env);
  return json({
    ok: true,
    token,
    user: {
      tgId: user.tgId,
      username: user.username,
      firstName: user.firstName,
      lastName: user.lastName || "",
      photoUrl: user.photoUrl || "",
      balance: microToUsdt(user.balanceMicro || 0),
    },
    rate,
  });
}

// ── User profile ─────────────────────────────────────────────
async function handleMe(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;
  const rate = await getRate(env);
  return json({
    ok: true,
    user: {
      tgId: user.tgId,
      username: user.username,
      firstName: user.firstName,
      photoUrl: user.photoUrl || "",
      balance: microToUsdt(user.balanceMicro || 0),
      banned: !!user.banned,
      createdAt: user.createdAt || 0,
    },
    rate,
  });
}

// ── Rate ─────────────────────────────────────────────────────
async function handleGetRate(req, env) {
  const rate = await getRate(env);
  return json({ ok: true, rate });
}

// ════════════════════════════════════════════════════════════
//  QR PAYMENT FLOW
// ════════════════════════════════════════════════════════════

// GET /api/qr/resolve-nspk?code=AS1234...
// Resolves NSPK/СБП QR code via CBR API (server-side, no PoW required from client)
async function handleResolveNspk(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const url = new URL(req.url);
  const code = String(url.searchParams.get("code") || "").trim();
  if (!code || !/^[A-Za-z0-9]{6,64}$/.test(code)) {
    return json({ ok: false, error: "Invalid code" }, 400);
  }

  const ua = "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36";

  // Method 1: CBR payment-link API
  try {
    const apiUrl = `https://upc.cbrpay.ru/v1/payment-link/${encodeURIComponent(code)}`;
    const res = await fetch(apiUrl, {
      headers: {
        "Accept": "application/json",
        "Origin": "https://qr.nspk.ru",
        "Referer": "https://qr.nspk.ru/",
        "User-Agent": ua
      }
    });
    if (res.ok) {
      const data = await res.json();
      const pd = data?.paymentDetails || data?.details || data;
      const amountRaw = pd?.amount ?? pd?.sum ?? pd?.totalAmount ?? null;
      let amountRub = 0;
      if (amountRaw !== null) {
        amountRub = Number(amountRaw);
        if (amountRub > 100000 && amountRub === Math.floor(amountRub)) amountRub /= 100;
      }
      if (amountRub > 0) {
        return json({
          ok: true, amountRub,
          name: pd?.payeeName ?? pd?.name ?? "",
          bankName: pd?.bankName ?? "",
          account: pd?.account ?? pd?.personalAcc ?? "",
          bic: pd?.bic ?? "",
        });
      }
    }
  } catch (_) {}

  // Method 2: Fetch qr.nspk.ru page, follow redirects, parse sum from final URL or HTML
  try {
    const nspkUrl = `https://qr.nspk.ru/${code}`;
    const res = await fetch(nspkUrl, {
      redirect: "follow",
      headers: { "User-Agent": ua, "Accept": "text/html,application/xhtml+xml,*/*" }
    });

    // Check final URL after redirects for sum param
    const finalUrl = new URL(res.url);
    const sumParam = finalUrl.searchParams.get("sum") || finalUrl.searchParams.get("amount");
    if (sumParam) {
      let amountRub = parseFloat(sumParam);
      if (amountRub > 100000 && amountRub === Math.floor(amountRub)) amountRub /= 100;
      if (amountRub > 0) {
        return json({ ok: true, amountRub, name: finalUrl.searchParams.get("name") || "" });
      }
    }

    // Parse HTML for embedded amount
    const html = await res.text();

    // JSON fields in page source
    const jsonMatch = html.match(/"amount"\s*:\s*(\d+\.?\d*)/i)
      || html.match(/"sum"\s*:\s*(\d+\.?\d*)/i)
      || html.match(/data-amount="(\d+\.?\d*)"/i)
      || html.match(/data-sum="(\d+\.?\d*)"/i);
    if (jsonMatch) {
      let amountRub = parseFloat(jsonMatch[1]);
      if (amountRub > 100000 && amountRub === Math.floor(amountRub)) amountRub /= 100;
      if (amountRub > 0) {
        const nameMatch = html.match(/"payeeName"\s*:\s*"([^"]+)"/i) || html.match(/"name"\s*:\s*"([^"]+)"/i);
        return json({ ok: true, amountRub, name: nameMatch ? nameMatch[1] : "" });
      }
    }

    // ST00012 format embedded in HTML
    const stMatch = html.match(/ST0001[12][^"'\s<>]+/i);
    if (stMatch) {
      const parts = stMatch[0].split("|");
      let amountRub = 0, name = "";
      for (const p of parts) {
        if (/^Sum=/i.test(p)) amountRub = parseInt(p.split("=")[1], 10) / 100;
        if (/^Name=/i.test(p)) name = p.split("=")[1];
      }
      if (amountRub > 0) return json({ ok: true, amountRub, name });
    }
  } catch (_) {}

  // Method 3: NSPK SBP API v1
  try {
    const res = await fetch(`https://api.nspk.ru/sbp/v1/qr/${code}/image-info`, {
      headers: { "Accept": "application/json", "User-Agent": ua }
    });
    if (res.ok) {
      const data = await res.json();
      const amountRaw = data?.amount ?? data?.sum ?? data?.paymentDetails?.amount ?? null;
      if (amountRaw !== null) {
        let amountRub = Number(amountRaw);
        if (amountRub > 100000 && amountRub === Math.floor(amountRub)) amountRub /= 100;
        if (amountRub > 0) return json({ ok: true, amountRub, name: data?.payeeName ?? data?.name ?? "" });
      }
    }
  } catch (_) {}

  return json({ ok: false, reason: "amount_not_found" });
}

// POST /api/qr/resolve  — universal QR amount resolver
// Accepts { qrData: "..." } with any QR content (URL, NSPK link, ST00012, etc.)
async function handleQrResolve(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  let qrData = "";
  if (req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    qrData = String(body.qrData || body.data || body.url || "").trim();
  } else {
    const u = new URL(req.url);
    qrData = String(u.searchParams.get("data") || u.searchParams.get("url") || "").trim();
  }
  if (!qrData) return bad("QR data required");

  const ua = "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36";

  // Helper: try to parse amount from kopecks vs rubles
  function normalizeAmount(raw) {
    let n = Number(raw);
    if (!n || !isFinite(n) || n <= 0) return 0;
    if (n > 100000 && n === Math.floor(n)) n /= 100;
    return n;
  }

  // ── 1. ST00011/ST00012 format ──
  if (/^ST0001[12]/i.test(qrData)) {
    const parts = qrData.split("|");
    let amountRub = 0, name = "", bankName = "", bic = "", account = "";
    for (const p of parts) {
      const eq = p.indexOf("=");
      if (eq < 0) continue;
      const k = p.slice(0, eq).toLowerCase(), v = p.slice(eq + 1);
      if (k === "sum") amountRub = parseInt(v, 10) / 100;
      if (k === "name" || k === "payeename") name = v;
      if (k === "bankname") bankName = v;
      if (k === "bic") bic = v;
      if (k === "personalacc") account = v;
    }
    if (amountRub > 0) return json({ ok: true, amountRub, name, bankName, account, bic, source: "st00012" });
  }

  // ── 2. NSPK / SBP URL → extract code → CBR + NSPK APIs ──
  const nspkMatch = qrData.match(/(?:qr\.nspk\.ru|qr\.sbp\.ru|sub\.nspk\.ru)\/([A-Za-z0-9]+)/i);
  if (nspkMatch) {
    const code = nspkMatch[1];
    // CBR payment-link API
    try {
      const res = await fetchWithTimeout(`https://upc.cbrpay.ru/v1/payment-link/${encodeURIComponent(code)}`, {
        headers: { Accept: "application/json", Origin: "https://qr.nspk.ru", Referer: "https://qr.nspk.ru/", "User-Agent": ua }
      }, 5000);
      if (res.ok) {
        const data = await res.json();
        const pd = data?.paymentDetails || data?.details || data;
        const amountRub = normalizeAmount(pd?.amount ?? pd?.sum ?? pd?.totalAmount);
        if (amountRub > 0) return json({ ok: true, amountRub, name: pd?.payeeName ?? pd?.name ?? "", bankName: pd?.bankName ?? "", source: "cbr" });
      }
    } catch {}
    // Fetch qr.nspk.ru page
    try {
      const res = await fetchWithTimeout(`https://qr.nspk.ru/${code}`, {
        redirect: "follow", headers: { "User-Agent": ua, Accept: "text/html,*/*" }
      }, 5000);
      const finalUrl = new URL(res.url);
      for (const k of ["sum", "amount", "Sum", "Amount"]) {
        const v = finalUrl.searchParams.get(k);
        if (v) { const a = normalizeAmount(v); if (a > 0) return json({ ok: true, amountRub: a, name: finalUrl.searchParams.get("name") || "", source: "nspk_redirect" }); }
      }
      const html = await res.text();
      const jm = html.match(/"amount"\s*:\s*(\d+\.?\d*)/i) || html.match(/"sum"\s*:\s*(\d+\.?\d*)/i) || html.match(/data-amount="(\d+\.?\d*)"/i);
      if (jm) { const a = normalizeAmount(jm[1]); if (a > 0) { const nm = html.match(/"payeeName"\s*:\s*"([^"]+)"/i) || html.match(/"name"\s*:\s*"([^"]+)"/i); return json({ ok: true, amountRub: a, name: nm ? nm[1] : "", source: "nspk_html" }); } }
      const stMatch = html.match(/ST0001[12][^"'\s<>]+/i);
      if (stMatch) { const pp = stMatch[0].split("|"); let ar = 0, nm = ""; for (const p of pp) { if (/^Sum=/i.test(p)) ar = parseInt(p.split("=")[1], 10) / 100; if (/^Name=/i.test(p)) nm = p.split("=")[1]; } if (ar > 0) return json({ ok: true, amountRub: ar, name: nm, source: "nspk_st" }); }
    } catch {}
    // NSPK SBP API
    try {
      const res = await fetchWithTimeout(`https://api.nspk.ru/sbp/v1/qr/${code}/image-info`, { headers: { Accept: "application/json", "User-Agent": ua } }, 3000);
      if (res.ok) { const d = await res.json(); const a = normalizeAmount(d?.amount ?? d?.sum ?? d?.paymentDetails?.amount); if (a > 0) return json({ ok: true, amountRub: a, name: d?.payeeName ?? d?.name ?? "", source: "nspk_api" }); }
    } catch {}
  }

  // ── 3. Any URL — fetch page, follow redirects, parse amount ──
  let isUrl = false;
  try { new URL(qrData); isUrl = true; } catch {}
  if (isUrl) {
    try {
      const res = await fetchWithTimeout(qrData, {
        redirect: "follow", headers: { "User-Agent": ua, Accept: "text/html,application/json,*/*" }
      }, 6000);
      // Check final URL params
      const finalUrl = new URL(res.url);
      for (const k of ["sum","Sum","amount","Amount","value","total","price","pay","money"]) {
        const v = finalUrl.searchParams.get(k);
        if (v) { const a = normalizeAmount(v); if (a > 0) return json({ ok: true, amountRub: a, name: finalUrl.searchParams.get("name") || "", source: "url_param" }); }
      }
      // Check if redirected to an NSPK URL
      const redirNspk = res.url.match(/(?:qr\.nspk\.ru|qr\.sbp\.ru)\/([A-Za-z0-9]+)/i);
      if (redirNspk) {
        const code2 = redirNspk[1];
        try {
          const r2 = await fetchWithTimeout(`https://upc.cbrpay.ru/v1/payment-link/${encodeURIComponent(code2)}`, { headers: { Accept: "application/json", Origin: "https://qr.nspk.ru", Referer: "https://qr.nspk.ru/", "User-Agent": ua } }, 4000);
          if (r2.ok) { const d = await r2.json(); const pd = d?.paymentDetails || d?.details || d; const a = normalizeAmount(pd?.amount ?? pd?.sum ?? pd?.totalAmount); if (a > 0) return json({ ok: true, amountRub: a, name: pd?.payeeName ?? pd?.name ?? "", source: "redirect_cbr" }); }
        } catch {}
      }
      // Parse HTML body
      const ct = res.headers.get("content-type") || "";
      const body = await res.text();
      // Try JSON-like fields
      const amtPatterns = [
        /"amount"\s*:\s*(\d+\.?\d*)/i, /"sum"\s*:\s*(\d+\.?\d*)/i,
        /"totalAmount"\s*:\s*(\d+\.?\d*)/i, /"price"\s*:\s*(\d+\.?\d*)/i,
        /data-amount="(\d+\.?\d*)"/i, /data-sum="(\d+\.?\d*)"/i,
        /data-price="(\d+\.?\d*)"/i, /data-total="(\d+\.?\d*)"/i,
        /class="[^"]*amount[^"]*"[^>]*>(\d[\d\s,.]*)/i,
        /class="[^"]*price[^"]*"[^>]*>(\d[\d\s,.]*)/i,
        /class="[^"]*sum[^"]*"[^>]*>(\d[\d\s,.]*)/i,
        /(?:Сумма|сумма|Итого|итого|К оплате|к оплате|Total|Amount)\s*[:=]?\s*([\d\s,.]+)\s*(?:₽|руб|RUB|rub)/i,
        /([\d\s,.]+)\s*(?:₽|руб\.?|RUB)\b/i,
      ];
      for (const pat of amtPatterns) {
        const m = body.match(pat);
        if (m) {
          const cleaned = String(m[1]).replace(/\s/g, "").replace(",", ".");
          const a = normalizeAmount(cleaned);
          if (a > 0 && a < 10000000) {
            const nm = body.match(/"payeeName"\s*:\s*"([^"]+)"/i) || body.match(/"merchant"\s*:\s*"([^"]+)"/i) || body.match(/"name"\s*:\s*"([^"]+)"/i);
            return json({ ok: true, amountRub: a, name: nm ? nm[1] : "", source: "html_parse" });
          }
        }
      }
      // ST00012 embedded in HTML
      const stm = body.match(/ST0001[12][^"'\s<>]+/i);
      if (stm) { const pp = stm[0].split("|"); let ar = 0, nm = ""; for (const p of pp) { if (/^Sum=/i.test(p)) ar = parseInt(p.split("=")[1], 10) / 100; if (/^Name=/i.test(p)) nm = p.split("=")[1]; } if (ar > 0) return json({ ok: true, amountRub: ar, name: nm, source: "html_st" }); }
    } catch {}
  }

  // ── 4. Fallback: regex for amount-like patterns in raw text ──
  const mSum = qrData.match(/(?:^|[^\w])Sum=(\d+\.?\d*)/i);
  if (mSum) { const a = normalizeAmount(mSum[1]); if (a > 0) return json({ ok: true, amountRub: a, source: "raw_sum" }); }
  const mAmt = qrData.match(/amount[=:](\d+\.?\d*)/i);
  if (mAmt) { const a = normalizeAmount(mAmt[1]); if (a > 0) return json({ ok: true, amountRub: a, source: "raw_amount" }); }

  return json({ ok: false, reason: "amount_not_found" });
}

// POST /api/qr/submit  (JSON: qrData + amountRub + optional image)
async function handleQrSubmit(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;
  if (user.banned) return bad("Account banned", 403);

  let amountRub = 0;
  let imageB64 = "";
  let note = "";
  let qrDataText = "";

  const ct = req.headers.get("content-type") || "";

  if (ct.includes("multipart/form-data")) {
    const fd = await req.formData();
    amountRub = Number(fd.get("amountRub") || 0);
    note = String(fd.get("note") || "").slice(0, 500);
    qrDataText = String(fd.get("qrData") || "").slice(0, 5000);
    const file = fd.get("image");
    if (file && file.size > 0) {
      const buf = await file.arrayBuffer();
      const bytes = new Uint8Array(buf);
      let binary = "";
      for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
      imageB64 = btoa(binary);
    }
  } else {
    const body = await req.json().catch(() => ({}));
    amountRub = Number(body.amountRub || 0);
    imageB64 = String(body.image || "");
    note = String(body.note || "").slice(0, 500);
    qrDataText = String(body.qrData || "").slice(0, 5000);
  }

  if (amountRub <= 0) return bad("Amount must be > 0");
  if (!qrDataText && !imageB64) return bad("QR data or image required");

  const rate = await getRate(env);
  if (rate <= 0) return bad("Rate unavailable, try later");

  const amountUsdt = amountRub / rate;
  const amountUsdtMicro = usdtToMicro(amountUsdt);
  const userBal = Number(user.balanceMicro || 0);

  if (userBal < amountUsdtMicro) {
    return bad(`Insufficient balance. Need ${amountUsdt.toFixed(2)} USDT, have ${microToUsdt(userBal).toFixed(2)} USDT`);
  }

  const qrId = uid();

  const qrRecord = {
    id: qrId,
    tgId: user.tgId,
    username: user.username,
    amountRub,
    rate,
    amountUsdt: Math.round(amountUsdt * 1e6) / 1e6,
    amountUsdtMicro,
    status: "PENDING",    // PENDING → PAID | REJECTED
    qrData: qrDataText,
    note,
    createdAt: now(),
    updatedAt: now(),
    paidAt: 0,
  };

  await kvPut(env, `qr:${qrId}`, qrRecord);
  if (imageB64) await kvPut(env, `qr_img:${qrId}`, imageB64);

  // Freeze USDT immediately
  await adjustBalance(env, user.tgId, -amountUsdtMicro, `QR freeze: ${qrId}`);

  // Notify admin
  await tgNotifyAdmins(env,
    `📱 <b>Новый QR на оплату!</b>\n\n` +
    `👤 @${user.username || user.tgId}\n` +
    `💵 <b>${amountRub.toLocaleString("ru-RU")} ₽</b>\n` +
    `💎 ${amountUsdt.toFixed(2)} USDT (курс ${rate.toFixed(2)})\n` +
    `📝 ${note || "—"}\n\n` +
    `🔗 ID: <code>${qrId}</code>\n` +
    `Откройте админку для сканирования QR.`
  );

  return json({ ok: true, qrId, amountUsdt: qrRecord.amountUsdt });
}

// GET /api/qr/history
async function handleQrHistory(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const keys = await kvList(env, "qr:");
  const items = [];
  for (const key of keys) {
    if (key.startsWith("qr_img:")) continue;
    const r = await kvGet(env, key);
    if (r && String(r.tgId) === String(user.tgId)) {
      items.push(r);
    }
  }
  items.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return json({ ok: true, items: items.slice(0, 50) });
}

// ════════════════════════════════════════════════════════════
//  WITHDRAWALS
// ════════════════════════════════════════════════════════════

// POST /api/withdraw
async function handleWithdrawRequest(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;
  if (user.banned) return bad("Account banned", 403);

  const body = await req.json().catch(() => ({}));
  const amountUsdt = Number(body.amountUsdt || 0);
  const destAddress = String(body.address || "").trim();
  const destNetwork = String(body.network || "TRC20").trim();

  if (amountUsdt <= 0) return bad("Amount must be > 0");
  if (!destAddress) return bad("Destination address required");

  const amountMicro = usdtToMicro(amountUsdt);
  const userBal = Number(user.balanceMicro || 0);
  if (userBal < amountMicro) return bad("Insufficient balance");

  const wdId = uid();
  const wdRecord = {
    id: wdId,
    tgId: user.tgId,
    username: user.username,
    amountUsdt,
    amountMicro,
    destAddress,
    destNetwork,
    status: "PENDING",   // PENDING → APPROVED | REJECTED
    txHash: "",
    createdAt: now(),
    updatedAt: now(),
  };

  await kvPut(env, `wd:${wdId}`, wdRecord);
  // Freeze balance
  await adjustBalance(env, user.tgId, -amountMicro, `WD freeze: ${wdId}`);

  await tgNotifyAdmins(env,
    `💸 <b>Запрос на вывод USDT</b>\n\n` +
    `👤 @${user.username || user.tgId}\n` +
    `💎 <b>${amountUsdt.toFixed(2)} USDT</b>\n` +
    `📤 ${destNetwork}: <code>${destAddress}</code>\n\n` +
    `ID: <code>${wdId}</code>`
  );

  return json({ ok: true, id: wdId });
}

// GET /api/withdraw/history
async function handleWithdrawHistory(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const keys = await kvList(env, "wd:");
  const items = [];
  for (const key of keys) {
    const r = await kvGet(env, key);
    if (r && String(r.tgId) === String(user.tgId)) items.push(r);
  }
  items.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return json({ ok: true, items: items.slice(0, 50) });
}

// ════════════════════════════════════════════════════════════
//  DEPOSITS (user views address, admin confirms)
// ════════════════════════════════════════════════════════════

// GET /api/deposit/info
async function handleDepositInfo(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;
  const cfg = await getSettings(env);
  return json({
    ok: true,
    wallet: cfg.usdtWallet || "",
    network: cfg.usdtNetwork || "TRC20",
  });
}

// GET /api/deposit/history
async function handleDepositHistory(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const keys = await kvList(env, "dep:");
  const items = [];
  for (const key of keys) {
    const r = await kvGet(env, key);
    if (r && String(r.tgId) === String(user.tgId)) items.push(r);
  }
  items.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return json({ ok: true, items: items.slice(0, 50) });
}

// ════════════════════════════════════════════════════════════
//  ADMIN ENDPOINTS
// ════════════════════════════════════════════════════════════

// GET /api/admin/qr/pending
async function handleAdminQrList(req, env) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const status = new URL(req.url).searchParams.get("status") || "";
  const keys = await kvList(env, "qr:");
  const items = [];
  for (const key of keys) {
    if (key.startsWith("qr_img:")) continue;
    const r = await kvGet(env, key);
    if (!r) continue;
    if (status && r.status !== status.toUpperCase()) continue;
    items.push(r);
  }
  items.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return json({ ok: true, items });
}

// GET /api/admin/qr/:id/image
async function handleAdminQrImage(req, env, qrId) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const b64 = await kvGet(env, `qr_img:${qrId}`);
  if (!b64) return bad("Image not found", 404);
  return json({ ok: true, image: b64 });
}

// POST /api/admin/qr/:id/paid
async function handleAdminQrPaid(req, env, qrId) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const qr = await kvGet(env, `qr:${qrId}`);
  if (!qr) return bad("QR not found", 404);
  if (qr.status !== "PENDING") return bad("Already processed: " + qr.status);

  qr.status = "PAID";
  qr.paidAt = now();
  qr.updatedAt = now();
  await kvPut(env, `qr:${qrId}`, qr);

  // Balance was already frozen on submit — no further deduction needed
  // Notify user
  await tgSend(env, qr.tgId,
    `✅ <b>Оплата прошла!</b>\n\n` +
    `💵 ${qr.amountRub.toLocaleString("ru-RU")} ₽\n` +
    `💎 −${qr.amountUsdt.toFixed(2)} USDT\n\n` +
    `Спасибо за покупку!`
  );

  return json({ ok: true });
}

// POST /api/admin/qr/:id/reject
async function handleAdminQrReject(req, env, qrId) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const body = await req.json().catch(() => ({}));
  const reason = String(body.reason || "").slice(0, 500);

  const qr = await kvGet(env, `qr:${qrId}`);
  if (!qr) return bad("QR not found", 404);
  if (qr.status !== "PENDING") return bad("Already processed: " + qr.status);

  qr.status = "REJECTED";
  qr.rejectReason = reason;
  qr.updatedAt = now();
  await kvPut(env, `qr:${qrId}`, qr);

  // Refund frozen balance
  try {
    await adjustBalance(env, qr.tgId, qr.amountUsdtMicro, `QR refund: ${qrId}`);
  } catch {}

  await tgSend(env, qr.tgId,
    `❌ <b>Оплата отклонена</b>\n\n` +
    `💵 ${qr.amountRub.toLocaleString("ru-RU")} ₽\n` +
    `💎 +${qr.amountUsdt.toFixed(2)} USDT (возврат)\n` +
    (reason ? `\n📝 Причина: ${reason}` : "")
  );

  return json({ ok: true });
}

// ── Admin: Users ─────────────────────────────────────────────
// GET /api/admin/users
async function handleAdminUsers(req, env) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const keys = await kvList(env, "u:");
  const users = [];
  for (const key of keys) {
    const u = await kvGet(env, key);
    if (u && u.tgId) users.push({
      tgId: u.tgId,
      username: u.username,
      firstName: u.firstName,
      balance: microToUsdt(u.balanceMicro || 0),
      banned: !!u.banned,
      exchangeEnabled: u.exchangeEnabled !== false,
      createdAt: u.createdAt || 0,
      updatedAt: u.updatedAt || 0,
    });
  }
  users.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return json({ ok: true, users });
}

// PATCH /api/admin/users/:tgId
async function handleAdminUserPatch(req, env, tgId) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const user = await getUser(env, tgId);
  if (!user) return bad("User not found", 404);

  const body = await req.json().catch(() => ({}));
  if (body.banned !== undefined) user.banned = !!body.banned;
  if (body.note !== undefined) user.note = String(body.note).slice(0, 1000);
  if (body.exchangeEnabled !== undefined) user.exchangeEnabled = !!body.exchangeEnabled;
  user.updatedAt = now();
  await saveUser(env, user);
  return json({ ok: true, exchangeEnabled: user.exchangeEnabled !== false });
}

// ── Admin: Withdrawals ───────────────────────────────────────
// GET /api/admin/withdrawals
async function handleAdminWithdrawals(req, env) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const keys = await kvList(env, "wd:");
  const items = [];
  for (const key of keys) {
    const r = await kvGet(env, key);
    if (r) items.push(r);
  }
  items.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return json({ ok: true, items });
}

// POST /api/admin/withdrawals/:id/approve
async function handleAdminWdApprove(req, env, wdId) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const body = await req.json().catch(() => ({}));
  const txHash = String(body.txHash || "").trim();

  const wd = await kvGet(env, `wd:${wdId}`);
  if (!wd) return bad("Not found", 404);
  if (wd.status !== "PENDING") return bad("Already processed: " + wd.status);

  wd.status = "APPROVED";
  wd.txHash = txHash;
  wd.updatedAt = now();
  await kvPut(env, `wd:${wdId}`, wd);

  await tgSend(env, wd.tgId,
    `✅ <b>Вывод одобрен!</b>\n\n` +
    `💎 ${wd.amountUsdt.toFixed(2)} USDT\n` +
    `📤 ${wd.destNetwork}: <code>${wd.destAddress}</code>\n` +
    (txHash ? `🔗 TX: <code>${txHash}</code>` : "")
  );
  return json({ ok: true });
}

// POST /api/admin/withdrawals/:id/reject
async function handleAdminWdReject(req, env, wdId) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const body = await req.json().catch(() => ({}));
  const reason = String(body.reason || "").slice(0, 500);

  const wd = await kvGet(env, `wd:${wdId}`);
  if (!wd) return bad("Not found", 404);
  if (wd.status !== "PENDING") return bad("Already processed: " + wd.status);

  wd.status = "REJECTED";
  wd.rejectReason = reason;
  wd.updatedAt = now();
  await kvPut(env, `wd:${wdId}`, wd);

  // Refund
  try {
    await adjustBalance(env, wd.tgId, wd.amountMicro, `WD refund: ${wdId}`);
  } catch {}

  await tgSend(env, wd.tgId,
    `❌ <b>Вывод отклонён</b>\n\n` +
    `💎 +${wd.amountUsdt.toFixed(2)} USDT (возврат)\n` +
    (reason ? `📝 ${reason}` : "")
  );
  return json({ ok: true });
}

// ── Admin: Deposits ──────────────────────────────────────────
// POST /api/admin/deposits/confirm
async function handleAdminDepositConfirm(req, env) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const body = await req.json().catch(() => ({}));
  const tgId = String(body.tgId || "").trim();
  const username = String(body.username || "").replace(/^@/, "").toLowerCase().trim();
  const amountUsdt = Number(body.amountUsdt || 0);
  const txHash = String(body.txHash || "").trim();

  if (amountUsdt <= 0) return bad("Amount must be > 0");

  let user;
  if (tgId) user = await getUser(env, tgId);
  else if (username) user = await getUserByUsername(env, username);
  if (!user) return bad("User not found", 404);

  const depId = uid();
  const amountMicro = usdtToMicro(amountUsdt);

  await adjustBalance(env, user.tgId, amountMicro, `Deposit: ${depId}`);

  const depRecord = {
    id: depId,
    tgId: user.tgId,
    username: user.username,
    amountUsdt,
    amountMicro,
    txHash,
    status: "CONFIRMED",
    createdAt: now(),
  };
  await kvPut(env, `dep:${depId}`, depRecord);

  await tgSend(env, user.tgId,
    `💰 <b>Пополнение зачислено!</b>\n\n` +
    `💎 +${amountUsdt.toFixed(2)} USDT\n` +
    (txHash ? `🔗 TX: <code>${txHash}</code>` : "")
  );

  return json({ ok: true, id: depId, newBalance: microToUsdt(await getBalance(env, user.tgId)) });
}

// GET /api/admin/deposits
async function handleAdminDeposits(req, env) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const keys = await kvList(env, "dep:");
  const items = [];
  for (const key of keys) {
    const r = await kvGet(env, key);
    if (r) items.push(r);
  }
  items.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return json({ ok: true, items });
}

// ── Admin: Settings ──────────────────────────────────────────
async function handleAdminGetSettings(req, env) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;
  const rate = await getRate(env);
  return json({ ok: true, settings: cfg, currentRate: rate });
}

async function handleAdminSaveSettings(req, env) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const body = await req.json().catch(() => ({}));
  const allowedKeys = [
    "adminTgIds", "usdtWallet", "usdtNetwork", "rateMode", "manualRate",
    "rateMarkupPercent", "botUsername", "siteName", "webAppUrl", "mainWorkerUrl", "mainAdminToken"
  ];

  const patch = {};
  for (const k of allowedKeys) {
    if (body[k] !== undefined) patch[k] = body[k];
  }

  const updated = await saveSettings(env, patch);
  return json({ ok: true, settings: updated });
}

// ── User: Stats ─────────────────────────────────────────────
async function handleUserStats(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const tgId = String(user.tgId);

  // QR payments
  const qrKeys = await kvList(env, "qr:");
  let qrCount = 0, qrSum = 0;
  for (const key of qrKeys) {
    if (key.startsWith("qr_img:")) continue;
    const r = await kvGet(env, key);
    if (r && String(r.tgId) === tgId) {
      qrCount++;
      if (r.status === "PAID") qrSum += Number(r.amountUsdt || 0);
    }
  }

  // Deposits
  const depKeys = await kvList(env, "dep:");
  let depCount = 0, depSum = 0;
  for (const key of depKeys) {
    const r = await kvGet(env, key);
    if (r && String(r.tgId) === tgId) {
      depCount++;
      depSum += Number(r.amountUsdt || 0);
    }
  }

  // Withdrawals
  const wdKeys = await kvList(env, "wd:");
  let wdCount = 0, wdSum = 0;
  for (const key of wdKeys) {
    const r = await kvGet(env, key);
    if (r && String(r.tgId) === tgId) {
      wdCount++;
      wdSum += Number(r.amountUsdt || 0);
    }
  }

  return json({
    ok: true,
    stats: {
      qrPayments: { count: qrCount, sumUsdt: Math.round(qrSum * 1e6) / 1e6 },
      deposits: { count: depCount, sumUsdt: Math.round(depSum * 1e6) / 1e6 },
      withdrawals: { count: wdCount, sumUsdt: Math.round(wdSum * 1e6) / 1e6 },
      createdAt: user.createdAt || 0,
      username: user.username || "",
      firstName: user.firstName || "",
      photoUrl: user.photoUrl || "",
    }
  });
}

// ── Admin: Stats (overview) ─────────────────────────────────
async function handleAdminStats(req, env) {
  const [cfg, err] = await requireAdmin(req, env);
  if (err) return err;

  const [userKeys, qrKeys, wdKeys, depKeys] = await Promise.all([
    kvList(env, "u:"),
    kvList(env, "qr:"),
    kvList(env, "wd:"),
    kvList(env, "dep:"),
  ]);

  // count QR by status
  let qrPending = 0, qrPaid = 0, qrRejected = 0, qrTotalRub = 0;
  for (const key of qrKeys) {
    if (key.startsWith("qr_img:")) continue;
    const r = await kvGet(env, key);
    if (!r) continue;
    if (r.status === "PENDING") qrPending++;
    if (r.status === "PAID") { qrPaid++; qrTotalRub += Number(r.amountRub || 0); }
    if (r.status === "REJECTED") qrRejected++;
  }

  let wdPending = 0, wdApproved = 0;
  for (const key of wdKeys) {
    const r = await kvGet(env, key);
    if (r?.status === "PENDING") wdPending++;
    if (r?.status === "APPROVED") wdApproved++;
  }

  const rate = await getRate(env);

  return json({
    ok: true,
    stats: {
      totalUsers: userKeys.length,
      qrPending, qrPaid, qrRejected, qrTotalRub,
      wdPending, wdApproved,
      totalDeposits: depKeys.length,
      currentRate: rate,
    }
  });
}

// ════════════════════════════════════════════════════════════
//  ROUTER
// ════════════════════════════════════════════════════════════
async function handleRequest(request, env, ctx) {
  if (request.method === "OPTIONS") return corsRes();

  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;

  try {
    // Bot webhook
    if (path === "/api/bot/webhook" && method === "POST")
      return await handleBotWebhook(request, env);

    // Auth
    if (path === "/api/auth/telegram" && method === "POST")
      return await handleTelegramAuth(request, env);
    if (path === "/api/request_code" && method === "POST")
      return await handleRequestCode(request, env);
    if (path === "/api/verify_code" && method === "POST")
      return await handleVerifyCode(request, env);

    // User
    if (path === "/api/me" && method === "GET")
      return await handleMe(request, env);
    if (path === "/api/rate" && method === "GET")
      return await handleGetRate(request, env);
    if (path === "/api/user/stats" && method === "GET")
      return await handleUserStats(request, env);

    // QR
    if (path === "/api/qr/submit" && method === "POST")
      return await handleQrSubmit(request, env);
    if (path === "/api/qr/history" && method === "GET")
      return await handleQrHistory(request, env);
    if (path === "/api/qr/resolve-nspk" && method === "GET")
      return await handleResolveNspk(request, env);
    if (path === "/api/qr/resolve" && (method === "POST" || method === "GET"))
      return await handleQrResolve(request, env);

    // Withdraw
    if (path === "/api/withdraw" && method === "POST")
      return await handleWithdrawRequest(request, env);
    if (path === "/api/withdraw/history" && method === "GET")
      return await handleWithdrawHistory(request, env);

    // Deposit
    if (path === "/api/deposit/info" && method === "GET")
      return await handleDepositInfo(request, env);
    if (path === "/api/deposit/history" && method === "GET")
      return await handleDepositHistory(request, env);

    // ── Admin routes ──
    if (path === "/api/admin/stats" && method === "GET")
      return await handleAdminStats(request, env);

    if (path === "/api/admin/qr" && method === "GET")
      return await handleAdminQrList(request, env);

    // /api/admin/qr/:id/image
    const mQrImg = path.match(/^\/api\/admin\/qr\/([^/]+)\/image$/);
    if (mQrImg && method === "GET")
      return await handleAdminQrImage(request, env, mQrImg[1]);

    // /api/admin/qr/:id/paid
    const mQrPaid = path.match(/^\/api\/admin\/qr\/([^/]+)\/paid$/);
    if (mQrPaid && method === "POST")
      return await handleAdminQrPaid(request, env, mQrPaid[1]);

    // /api/admin/qr/:id/reject
    const mQrRej = path.match(/^\/api\/admin\/qr\/([^/]+)\/reject$/);
    if (mQrRej && method === "POST")
      return await handleAdminQrReject(request, env, mQrRej[1]);

    // Admin users
    if (path === "/api/admin/users" && method === "GET")
      return await handleAdminUsers(request, env);
    const mUserPatch = path.match(/^\/api\/admin\/users\/([^/]+)$/);
    if (mUserPatch && method === "PATCH")
      return await handleAdminUserPatch(request, env, mUserPatch[1]);

    // Admin withdrawals
    if (path === "/api/admin/withdrawals" && method === "GET")
      return await handleAdminWithdrawals(request, env);
    const mWdAppr = path.match(/^\/api\/admin\/withdrawals\/([^/]+)\/approve$/);
    if (mWdAppr && method === "POST")
      return await handleAdminWdApprove(request, env, mWdAppr[1]);
    const mWdRej = path.match(/^\/api\/admin\/withdrawals\/([^/]+)\/reject$/);
    if (mWdRej && method === "POST")
      return await handleAdminWdReject(request, env, mWdRej[1]);

    // Admin deposits
    if (path === "/api/admin/deposits" && method === "GET")
      return await handleAdminDeposits(request, env);
    if (path === "/api/admin/deposits/confirm" && method === "POST")
      return await handleAdminDepositConfirm(request, env);

    // Admin settings
    if (path === "/api/admin/settings" && method === "GET")
      return await handleAdminGetSettings(request, env);
    if (path === "/api/admin/settings" && method === "POST")
      return await handleAdminSaveSettings(request, env);

    // Deposit request (user creates pending deposit with unique amount)
    if (path === "/api/deposit/request" && method === "POST")
      return await handleDepositRequest(request, env);
    if (path === "/api/deposit/status" && method === "GET")
      return await handleDepositStatus(request, env);

    // ── Exchange (P2P buy via crossflag) ──────────────────────
    if (path === "/api/exchange/rate" && method === "GET")
      return await handleExchangeRate(request, env);
    if (path === "/api/exchange/offers" && method === "GET")
      return await handleExchangeOffers(request, env);
    if (path === "/api/exchange/locks" && method === "GET")
      return await handleExchangeLocks(request, env);
    if (path === "/api/exchange/reserve" && method === "POST")
      return await handleExchangeReserve(request, env);
    if (path === "/api/exchange/mark_paid" && method === "POST")
      return await handleExchangeMarkPaid(request, env);
    if (path === "/api/exchange/cancel" && method === "POST")
      return await handleExchangeCancel(request, env);
    if (path === "/api/exchange/history" && method === "GET")
      return await handleExchangeHistory(request, env);
    const mExchDeal = path.match(/^\/api\/exchange\/deal\/([^/]+)$/);
    if (mExchDeal && method === "GET")
      return await handleExchangeDeal(request, env, mExchDeal[1]);

    if (path === "/api/exchange/buy_request" && method === "POST")
      return await handleExchangeBuyRequest(request, env);
    if (path === "/api/exchange/buy_request/cancel" && method === "POST")
      return await handleExchangeBuyRequestCancel(request, env);
    if (path === "/api/exchange/buy_requests" && method === "GET")
      return await handleExchangeBuyRequests(request, env);
    if (path === "/api/exchange/submit_proof" && method === "POST")
      return await handleExchangeSubmitProof(request, env);
    // Webhook called by crossflag when a matching offer is found for a MW request
    if (path === "/api/internal/mw_match_notify" && method === "POST")
      return await handleMwMatchNotify(request, env);
    // Webhook called by crossflag when admin confirms deal SUCCESS — credits MW user balance
    if (path === "/api/internal/mw_deal_complete" && method === "POST")
      return await handleMwDealComplete(request, env);
    if (path === "/api/admin/exchange_requests" && method === "GET")
      return await handleAdminExchangeRequests(request, env);

    return bad("Not found", 404);
  } catch (e) {
    return bad("Internal error: " + (e.message || String(e)), 500);
  }
}

// ════════════════════════════════════════════════════════════
//  RAPIRA API — auto-deposit via merchant API
// ════════════════════════════════════════════════════════════

// ── Crypto helpers ───────────────────────────────────────────
function b64urlFromBytes(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function b64ToBytes(raw) {
  let s = String(raw || "").trim();

  // Case 1: raw PEM text (starts with -----)
  if (s.startsWith("-----")) {
    s = s.replace(/-----[^-]+-----/g, "").replace(/\s+/g, "");
    const bin = atob(s);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  // Case 2: bare base64 — decode once
  const bin = atob(s.replace(/\s+/g, ""));

  // Case 2a: decoded result is PEM text (secret was base64-of-PEM)
  if (bin.startsWith("-----")) {
    let s2 = bin.replace(/-----[^-]+-----/g, "").replace(/\s+/g, "");
    const bin2 = atob(s2);
    const out = new Uint8Array(bin2.length);
    for (let i = 0; i < bin2.length; i++) out[i] = bin2.charCodeAt(i);
    return out;
  }

  // Case 2b: decoded result is raw DER bytes — use directly
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// DER length encoding helper
function derLen(n) {
  if (n < 128) return new Uint8Array([n]);
  if (n < 256) return new Uint8Array([0x81, n]);
  return new Uint8Array([0x82, (n >> 8) & 0xff, n & 0xff]);
}

// Concatenate multiple Uint8Arrays
function concatBytes(...arrays) {
  const total = arrays.reduce((s, a) => s + a.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const a of arrays) { out.set(a, off); off += a.length; }
  return out;
}

// Wrap PKCS1 RSA private key DER bytes into a PKCS8 DER envelope
function pkcs1ToPkcs8(pkcs1) {
  // RSA OID: 1.2.840.113549.1.1.1
  const oid = new Uint8Array([0x2a,0x86,0x48,0x86,0xf7,0x0d,0x01,0x01,0x01]);
  // OID TLV: tag(1) + len(1) + value(9) = 11 bytes
  const oidTlv = concatBytes(new Uint8Array([0x06]), derLen(oid.length), oid);
  // NULL TLV
  const nullTlv = new Uint8Array([0x05, 0x00]);
  // AlgorithmIdentifier SEQUENCE { oidTlv, nullTlv }
  const algContent = concatBytes(oidTlv, nullTlv);
  const algId = concatBytes(new Uint8Array([0x30]), derLen(algContent.length), algContent);
  // OCTET STRING { pkcs1 }
  const octet = concatBytes(new Uint8Array([0x04]), derLen(pkcs1.length), pkcs1);
  // version INTEGER 0
  const version = new Uint8Array([0x02, 0x01, 0x00]);
  // PrivateKeyInfo SEQUENCE { version, algId, octet }
  const inner = concatBytes(version, algId, octet);
  return concatBytes(new Uint8Array([0x30]), derLen(inner.length), inner);
}

async function importRsaPrivateKey(keyBytes) {
  const alg = { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" };
  // Try as-is (PKCS8) first
  try {
    return await crypto.subtle.importKey("pkcs8", keyBytes, alg, false, ["sign"]);
  } catch (e1) {
    // Assume PKCS1, wrap in PKCS8 envelope and retry
    try {
      const pkcs8 = pkcs1ToPkcs8(keyBytes);
      return await crypto.subtle.importKey("pkcs8", pkcs8, alg, false, ["sign"]);
    } catch (e2) {
      throw new Error(`RSA key import failed. PKCS8 err: ${e1.message}. PKCS1-wrap err: ${e2.message}. Key length: ${keyBytes.length} bytes, first bytes: ${[...keyBytes.slice(0,4)].map(b=>b.toString(16)).join(' ')}`);
    }
  }
}

async function createRapiraClientJwt(env) {
  const te = new TextEncoder();
  const rawKey = String(env.RAPIRA_PRIVATE_KEY || "").trim();
  if (!rawKey) throw new Error("RAPIRA_PRIVATE_KEY not set");

  const keyBytes = b64ToBytes(rawKey);
  const privateKey = await importRsaPrivateKey(keyBytes);

  const nowSec = Math.floor(Date.now() / 1000);
  const header = { typ: "JWT", alg: "RS256" };
  const payload = { exp: nowSec + 3600, jti: crypto.randomUUID().replace(/-/g, "") };

  const h = b64urlFromBytes(te.encode(JSON.stringify(header)));
  const p = b64urlFromBytes(te.encode(JSON.stringify(payload)));
  const signingInput = `${h}.${p}`;

  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    privateKey,
    te.encode(signingInput)
  );

  return `${signingInput}.${b64urlFromBytes(new Uint8Array(signature))}`;
}

async function getRapiraBearer(env) {
  const cacheKey = "rapira:bearer";
  const cached = await kvGet(env, cacheKey);
  if (cached && cached.token && Number(cached.expiresAt) > Date.now() + 60000) {
    return cached.token;
  }

  const clientJwt = await createRapiraClientJwt(env);
  const kid = String(env.RAPIRA_KID || "");

  const res = await fetch("https://api.rapira.net/open/generate_jwt", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify({ kid, jwt_token: clientJwt }),
  });

  const data = await res.json();
  if (!data.token) throw new Error("Rapira bearer: no token: " + JSON.stringify(data).slice(0, 200));

  const token = String(data.token);
  const expMs = Date.now() + 3600 * 1000;
  await kvPut(env, cacheKey, { token, expiresAt: expMs }, { expirationTtl: 3600 });
  return token;
}

async function rapiraFetch(env, path, opts = {}) {
  const bearer = await getRapiraBearer(env);
  let url = "https://api.rapira.net" + path;

  if (opts.query) {
    url += (url.includes("?") ? "&" : "?") + new URLSearchParams(opts.query).toString();
  }

  const headers = {
    "Authorization": "Bearer " + bearer,
    "Accept": "application/json",
    ...(opts.headers || {}),
  };

  let body = undefined;
  if (opts.form) {
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    body = new URLSearchParams(opts.form).toString();
  } else if (opts.json !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(opts.json);
  }

  const r = await fetch(url, { method: opts.method || "GET", headers, body });
  if (!r.ok) {
    const txt = await r.text();
    throw new Error(`Rapira ${path} → ${r.status}: ${txt.slice(0, 200)}`);
  }
  return r.json();
}

// Extract deposit list from various Rapira response shapes
function rapiraDepositItems(data) {
  if (Array.isArray(data?.data?.content)) return data.data.content;
  if (Array.isArray(data?.content)) return data.content;
  if (Array.isArray(data?.data)) return data.data;
  if (Array.isArray(data)) return data;
  return [];
}

// Credit a Rapira deposit to a user (idempotent via rapira_credited: key)
async function creditRapiraDeposit(env, user, dep) {
  const txid = String(dep?.txid || dep?.txHash || dep?.id || "").trim();
  if (!txid) return false;

  const dupKey = `rapira_credited:${txid}`;
  const already = await kvGet(env, dupKey);
  if (already) return false; // already credited

  const amountUsdt = Number(dep?.amount || dep?.amountUsdt || 0);
  if (amountUsdt <= 0) return false;
  const amountMicro = usdtToMicro(amountUsdt);

  try {
    await adjustBalance(env, user.tgId, amountMicro, `Rapira deposit: ${txid}`);
  } catch { return false; }

  // Mark as credited (30 days TTL)
  await kvPut(env, dupKey, { tgId: user.tgId, ts: now() }, { expirationTtl: 86400 * 30 });

  // Record in deposit history
  const depId = uid();
  await kvPut(env, `dep:${depId}`, {
    id: depId,
    tgId: user.tgId,
    username: user.username,
    amountUsdt,
    amountMicro,
    txHash: txid,
    status: "CONFIRMED",
    auto: true,
    source: "rapira",
    createdAt: now(),
  });

  // Notify user
  try {
    await tgSend(env, user.tgId,
      `💰 <b>Депозит зачислен!</b>\n\n` +
      `💎 +${amountUsdt.toFixed(2)} USDT\n` +
      `🔗 TX: <code>${txid.slice(0, 20)}...</code>`
    );
  } catch {}

  // Notify admins
  try {
    await tgNotifyAdmins(env,
      `✅ <b>Автопополнение (Rapira)</b>\n\n` +
      `👤 @${user.username || user.tgId}\n` +
      `💎 +${amountUsdt.toFixed(2)} USDT\n` +
      `🔗 <code>${txid}</code>`
    );
  } catch {}

  return true;
}

// ── POST /api/deposit/request ────────────────────────────────
// Returns (or creates) the user's personal Rapira USDT-TRC20 deposit address.
async function handleDepositRequest(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  // Reuse existing address if already created
  if (user.rapiraDepositAddress) {
    return json({
      ok: true,
      wallet: user.rapiraDepositAddress,
      network: "TRC20",
      expiresIn: 3600,
    });
  }

  try {
    const data = await rapiraFetch(env, "/open/deposit_address", {
      method: "POST",
      query: { currency: "usdt-trc20" },
    });

    const address = String(
      data?.address || data?.data?.address || data?.wallet || ""
    ).trim();
    if (!address) throw new Error("No address in response: " + JSON.stringify(data).slice(0, 200));

    user.rapiraDepositAddress = address;
    user.updatedAt = now();
    await saveUser(env, user);

    return json({ ok: true, wallet: address, network: "TRC20", expiresIn: 3600 });
  } catch (e) {
    return bad("Failed to create deposit address: " + e.message, 500);
  }
}

// ── GET /api/deposit/status ──────────────────────────────────
// Polls Rapira for the user's deposit address and credits on SUCCESS.
async function handleDepositStatus(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const address = String(user.rapiraDepositAddress || "").trim();
  if (!address) return json({ ok: true, status: "NONE" });

  try {
    const data = await rapiraFetch(env, "/open/deposit/records", {
      method: "POST",
      form: { pageNo: 0, pageSize: 20, address },
    });

    const items = rapiraDepositItems(data);

    for (const dep of items) {
      const st = String(dep?.status || "").toUpperCase();

      if (st === "SUCCESS") {
        const credited = await creditRapiraDeposit(env, user, dep);
        const amountUsdt = Number(dep?.amount || dep?.amountUsdt || 0);
        return json({ ok: true, status: "CREDITED", amountUsdt });
      }

      if (["MEMPOOL", "PENDING_CONFIRMATIONS", "PENDING_AML", "PENDING"].includes(st)) {
        return json({ ok: true, status: "PENDING", amountUsdt: Number(dep?.amount || 0) });
      }
    }

    return json({ ok: true, status: "NONE" });
  } catch (e) {
    return json({ ok: true, status: "NONE", _error: e.message });
  }
}

// ── Cron: scan Rapira deposits for all users ─────────────────
async function scanRapiraDeposits(env) {
  const userKeys = await kvList(env, "u:");

  for (const key of userKeys) {
    const user = await kvGet(env, key);
    if (!user || !user.rapiraDepositAddress) continue;

    try {
      const data = await rapiraFetch(env, "/open/deposit/records", {
        method: "POST",
        form: { pageNo: 0, pageSize: 20, address: user.rapiraDepositAddress },
      });

      const items = rapiraDepositItems(data);

      for (const dep of items) {
        const st = String(dep?.status || "").toUpperCase();
        if (st !== "SUCCESS") continue;
        await creditRapiraDeposit(env, user, dep);
      }
    } catch {}
  }
}

// ════════════════════════════════════════════════════════════
//  EXCHANGE — P2P buy via crossflag
// ════════════════════════════════════════════════════════════

const CROSSFLAG_BASE = "https://api.crossflag.org";

async function cfGet(path) {
  const res = await fetch(CROSSFLAG_BASE + path, { headers: { Accept: "application/json" } });
  return res.json().catch(() => ({ ok: false, error: "Crossflag unavailable" }));
}

async function cfPost(path, body) {
  const res = await fetch(CROSSFLAG_BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  return res.json().catch(() => ({ ok: false, error: "Crossflag unavailable" }));
}

// Ensure user has a Rapira deposit address, create one if missing
async function ensureDepositAddress(env, user) {
  if (user.rapiraDepositAddress) return user.rapiraDepositAddress;
  try {
    const data = await rapiraFetch(env, "/open/deposit_address", { method: "POST", query: { currency: "usdt-trc20" } });
    const address = String(data?.address || data?.data?.address || "").trim();
    if (address) {
      user.rapiraDepositAddress = address;
      user.updatedAt = now();
      await saveUser(env, user);
    }
    return address;
  } catch { return ""; }
}

async function handleExchangeRate(req, env) {
  const d = await cfGet("/api/public/rapira_rate");
  return json(d);
}

async function handleExchangeOffers(req, env) {
  let tgId = "";
  try {
    const token = authFromReq(req);
    if (token) {
      const sessId = await kvGet(env, `sess:${token}`);
      if (sessId) tgId = String(sessId);
    }
  } catch {}
  const allOffers = await getMwOffers(env);
  const visible = allOffers.filter(o => {
    if (o.frozen) return false;
    if (String(o.status || "NEW").toUpperCase() !== "NEW") return false;
    if (o.mwOnly || o.mwTgId) return tgId && String(o.mwTgId || "") === tgId;
    return true;
  });
  return json({ ok: true, offers: visible });
}

async function handleExchangeLocks(req, env) {
  const url = new URL(req.url);
  const ids = url.searchParams.get("ids") || "";
  const d = await cfGet("/api/public/buy_lock_status?ids=" + encodeURIComponent(ids));
  return json(d);
}

async function handleExchangeReserve(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;
  if (user.exchangeEnabled === false) return bad("Обмен недоступен для вашего аккаунта. Обратитесь к администратору.", 403);

  const body = await req.json().catch(() => ({}));
  const offerId = String(body.id || "").trim();
  if (!offerId) return bad("Missing offer id");

  const address = await ensureDepositAddress(env, user);
  const allOffers = await getMwOffers(env);
  const oi = allOffers.findIndex(o => o.id === offerId);
  if (oi < 0) return bad("Offer not found", 404);
  const offer = allOffers[oi];
  if (String(offer.status || "NEW").toUpperCase() !== "NEW") return bad("Offer already taken");
  if (offer.mwOnly && offer.mwTgId && String(offer.mwTgId) !== String(user.tgId)) return bad("Offer not available");

  const reserveId = uid();
  const expiresAt = now() + 20 * 60 * 1000;
  allOffers[oi] = { ...offer, status: "RESERVED", reservedBy: user.tgId, reserveId, reservedAt: now(), expiresAt };
  await saveMwOffers(env, allOffers);

  const dealKey = `exch:${user.tgId}:${offerId}`;
  await kvPut(env, dealKey, {
    offerId, reserveId, expiresAt,
    amountRub: offer.amountRub, rate: offer.rate,
    payBank: offer.payBank || "", payRequisite: offer.payRequisite || "",
    method: offer.method || "", address,
    status: "RESERVED", createdAt: now(), updatedAt: now(),
    createdFromChatId: offer.createdFromChatId || "",
  });
  const idxKey = `exch_idx:${user.tgId}`;
  const idxList = (await kvGet(env, idxKey)) || [];
  if (!idxList.includes(offerId)) idxList.unshift(offerId);
  await kvPut(env, idxKey, idxList.slice(0, 50));

  // Notify originating chat that offer was taken
  const notifyChat = offer.createdFromChatId || env.TG_CHAT_ID;
  if (notifyChat) {
    const uname = user.firstName || user.username || String(user.tgId);
    const usernameStr = user.username ? ` (@${escHtml(user.username)})` : "";
    await tgSend(env, notifyChat,
      `📱 <b>Клиент взял заявку в работу</b>\n\n` +
      `👤 ${escHtml(uname)}${usernameStr}\n` +
      `💳 ${fmtRubTg(offer.amountRub)} ₽ → ${(offer.amountRub / offer.rate).toFixed(2)} USDT\n` +
      `🏦 ${escHtml(offer.payBank || "")} | ${escHtml(offer.payRequisite || "")}\n` +
      `Offer: <code>${offerId}</code>`
    ).catch(() => {});
  }

  return json({
    ok: true, reserveId, expiresAt, address,
    offer: { id: offerId, amountRub: offer.amountRub, rate: offer.rate, payBank: offer.payBank || "", payRequisite: offer.payRequisite || "", method: offer.method || "" },
  });
}

async function handleExchangeMarkPaid(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const body = await req.json().catch(() => ({}));
  const offerId = String(body.id || "").trim();
  if (!offerId) return bad("Missing offer id");

  const dealKey = `exch:${user.tgId}:${offerId}`;
  const deal = (await kvGet(env, dealKey)) || {};
  await kvPut(env, dealKey, { ...deal, status: "PAID", paidAt: now(), updatedAt: now() });

  // Notify originating chat
  const notifyChat = deal.createdFromChatId || env.TG_CHAT_ID;
  if (notifyChat) {
    const uname = user.firstName || user.username || String(user.tgId);
    const usernameStr = user.username ? ` (@${escHtml(user.username)})` : "";
    await tgSend(env, notifyChat,
      `💳 <b>Клиент отметил оплату</b>\n\n` +
      `👤 ${escHtml(uname)}${usernameStr}\n` +
      `💰 ${fmtRubTg(Number(deal.amountRub || 0))} ₽\n` +
      `🏦 ${escHtml(deal.payBank || "")} | ${escHtml(deal.payRequisite || "")}\n` +
      `Offer: <code>${offerId}</code>`
    ).catch(() => {});
  }

  return json({ ok: true });
}

async function handleExchangeCancel(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const body = await req.json().catch(() => ({}));
  const offerId = String(body.id || "").trim();
  if (!offerId) return bad("Missing offer id");

  const dealKey = `exch:${user.tgId}:${offerId}`;
  const deal = (await kvGet(env, dealKey)) || {};
  await kvPut(env, dealKey, { ...deal, status: "CANCELLED", cancelledAt: now(), updatedAt: now() });

  // Release offer back to NEW so it can be taken by someone else
  const allOffers = await getMwOffers(env);
  const oi = allOffers.findIndex(o => o.id === offerId && String(o.reservedBy || "") === String(user.tgId));
  if (oi >= 0) {
    allOffers[oi] = { ...allOffers[oi], status: "NEW", reservedBy: null, reserveId: null, reservedAt: null };
    await saveMwOffers(env, allOffers);
  }
  return json({ ok: true });
}

async function handleExchangeHistory(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const idxKey = `exch_idx:${user.tgId}`;
  const idx = (await kvGet(env, idxKey)) || [];
  const deals = [];
  for (const offerId of idx.slice(0, 30)) {
    const d = await kvGet(env, `exch:${user.tgId}:${offerId}`);
    if (d) deals.push(d);
  }
  return json({ ok: true, deals });
}

async function handleExchangeDeal(req, env, offerId) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;
  const deal = await kvGet(env, `exch:${user.tgId}:${offerId}`);
  if (!deal) return bad("Deal not found", 404);
  return json({ ok: true, deal });
}

const BUY_REQ_TTL_MS = 7 * 60 * 1000; // 7 minutes, matches crossflag

async function handleExchangeBuyRequest(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;
  if (user.exchangeEnabled === false) return bad("Обмен недоступен для вашего аккаунта. Обратитесь к администратору.", 403);

  const body = await req.json().catch(() => ({}));
  const minRub = Math.floor(Number(body.minRub ?? body.amountRub ?? body.amount ?? 0));
  const maxRub = Math.floor(Number(body.maxRub ?? body.amountRub ?? body.amount ?? minRub));
  const count = Math.max(1, Math.min(10, Math.floor(Number(body.count ?? 1))));
  if (!minRub || minRub < 100) return bad("Минимальная сумма 100 ₽");
  if (maxRub < minRub) return bad("Максимум должен быть ≥ минимума");
  if (maxRub > 5_000_000) return bad("Слишком большая сумма");

  const address = await ensureDepositAddress(env, user);
  const reqId = "mwr_" + uid();
  const reqRec = {
    id: reqId,
    tgId: user.tgId,
    name: user.firstName || user.name || user.username || "",
    minRub, maxRub, count,
    address,
    status: "PENDING",
    matchedOffersCount: 0,
    matchedOfferIds: [],
    createdAt: now(),
  };
  await kvPut(env, `buyreq:${user.tgId}:${reqId}`, reqRec);

  const idxKey = `buyreq_idx:${user.tgId}`;
  const idx = (await kvGet(env, idxKey)) || [];
  idx.unshift(reqId);
  await kvPut(env, idxKey, idx.slice(0, 50));

  // Post buy request to all registered chats
  try {
    const uname = user.firstName || user.username || String(user.tgId);
    const usernameStr = user.username ? ` (@${user.username})` : "";
    const rateNow = await getRate(env);
    const approxUsdt = rateNow > 0
      ? `≈ ${(minRub / rateNow).toFixed(2)} — ${(maxRub / rateNow).toFixed(2)} USDT`
      : "";
    const midAmount = Math.round((minRub + maxRub) / 2);
    const exampleRate = rateNow > 0 ? rateNow.toFixed(1) : "90.0";
    const msgText =
      `📥 <b>Новый запрос на покупку USDT</b>\n\n` +
      `👤 ${escHtml(uname)}${escHtml(usernameStr)}\n` +
      `💳 Сумма: <b>${fmtRubTg(minRub)} — ${fmtRubTg(maxRub)} ₽</b>\n` +
      (approxUsdt ? `💵 ${approxUsdt}\n` : "") +
      `📦 Ордеров: <b>${count}</b>\n\n` +
      `<i>Ответьте на это сообщение в формате:</i>\n` +
      `<code>сумма курс реквизит банк</code>\n` +
      `Пример: <code>${midAmount} ${exampleRate} 79001234567 Сбербанк</code>\n\n` +
      `ID: <code>${reqId}</code>`;

    // Collect all target chats: env.TG_CHAT_ID + mwBuyAmountChats KV
    const extraChats = (await kvGet(env, "mwBuyAmountChats")) || [];
    const allChats = [...new Set([
      ...(env.TG_CHAT_ID ? [String(env.TG_CHAT_ID)] : []),
      ...extraChats.map(String),
    ])];

    for (const chatTarget of allChats) {
      try {
        const sent = await tgSend(env, chatTarget, msgText);
        if (sent?.result?.message_id) {
          const msgId = sent.result.message_id;
          await kvPut(env, `mwreq_msg:${chatTarget}:${msgId}`, { reqId, tgId: user.tgId, minRub, maxRub, count }, { expirationTtl: 7200 });
          // Also store without chatTarget prefix for backwards compat
          await kvPut(env, `mwreq_msg:${msgId}`, { reqId, tgId: user.tgId, minRub, maxRub, count }, { expirationTtl: 7200 });
          if (!reqRec.adminMsgId) {
            reqRec.adminMsgId = msgId;
            await kvPut(env, `buyreq:${user.tgId}:${reqId}`, reqRec);
          }
        }
      } catch {}
    }
  } catch {}

  return json({ ok: true, id: reqId, status: "PENDING", minRub, maxRub, count, approxRate: 0, createdAt: reqRec.createdAt });
}

async function handleExchangeBuyRequestCancel(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const body = await req.json().catch(() => ({}));
  const id = String(body.id || "").trim();
  if (!id) return bad("Missing id");

  const rec = await kvGet(env, `buyreq:${user.tgId}:${id}`);
  if (!rec) return bad("Not found", 404);
  if (String(rec.tgId) !== String(user.tgId)) return bad("Forbidden", 403);

  rec.status = "CANCELLED";
  rec.updatedAt = now();
  await kvPut(env, `buyreq:${user.tgId}:${id}`, rec);
  return json({ ok: true, id, status: "CANCELLED" });
}

async function handleExchangeBuyRequests(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const idxKey = `buyreq_idx:${user.tgId}`;
  const idx = (await kvGet(env, idxKey)) || [];
  const items = [];
  for (const id of idx.slice(0, 20)) {
    const r = await kvGet(env, `buyreq:${user.tgId}:${id}`);
    if (!r) continue;
    let st = r.status || "PENDING";
    if (st === "PENDING" && now() - Number(r.createdAt || 0) > BUY_REQ_TTL_MS) st = "EXPIRED";
    items.push({ ...r, status: st });
  }
  return json({ ok: true, items });
}

async function handleAdminExchangeRequests(req, env) {
  const adminToken = req.headers.get("X-Admin-Token") || req.headers.get("x-admin-token") || "";
  const cfg = await getSettings(env);
  if (!adminToken || adminToken !== cfg.adminToken) return bad("Unauthorized", 401);

  // List all exchange buy requests via KV list
  const keys = await env.KV.list({ prefix: "buyreq:", limit: 200 });
  const reqs = [];
  for (const k of keys.keys || []) {
    if (k.name.split(":").length === 3) { // buyreq:{tgId}:{reqId}
      const r = await kvGet(env, k.name);
      if (r) reqs.push(r);
    }
  }
  reqs.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  return json({ ok: true, requests: reqs.slice(0, 200) });
}

// Accept proof upload and send to MW admin chat with ✅/❌ buttons
async function handleExchangeSubmitProof(req, env) {
  const [user, err] = await requireUser(req, env);
  if (err) return err;

  const ct = req.headers.get("Content-Type") || "";
  if (!ct.includes("multipart/form-data")) return bad("Expected multipart/form-data");

  let fd;
  try { fd = await req.formData(); } catch { return bad("Invalid form data"); }

  const offerId = String(fd.get("offerId") || fd.get("id") || "").trim();
  if (!offerId) return bad("Missing offerId");

  // Client sends "photo" (screenshot) and "pdf1" (PDF receipt)
  const photoFile = fd.get("photo") || fd.get("proof") || fd.get("image") || null;
  const pdfFile   = fd.get("pdf1")  || fd.get("pdf")   || fd.get("file")  || null;

  // Update deal status
  const dealKey = `exch:${user.tgId}:${offerId}`;
  const deal = (await kvGet(env, dealKey)) || {};
  await kvPut(env, dealKey, { ...deal, status: "PROOF_SUBMITTED", proofAt: now(), updatedAt: now() });

  const amountRub = Number(deal.amountRub || 0);
  const rate = Number(deal.rate || 0);
  const usdtStr = rate > 0 ? (amountRub / rate).toFixed(2) : "?";
  const uname = user.firstName || user.username || String(user.tgId);
  const usernameStr = user.username ? ` (@${escHtml(user.username)})` : "";
  const shortId = String(offerId).slice(-8);

  const summaryCaption =
    `📋 <b>Клиент прислал доказательства оплаты по BUY ${shortId}</b>\n\n` +
    `👤 ${escHtml(uname)}${usernameStr}\n` +
    `💰 Сумма: <b>${fmtRubTg(amountRub)} ₽</b> (≈ ${usdtStr} USDT)\n` +
    `🏦 ${escHtml(deal.payBank || "")} | ${escHtml(deal.payRequisite || "")}\n` +
    `\nОffer: <code>${offerId}</code>`;

  const kb = { inline_keyboard: [[
    { text: "✅ Успешно", callback_data: `mwbuy:ok:${offerId}:${user.tgId}` },
    { text: "❌ Ошибка",  callback_data: `mwbuy:err:${offerId}:${user.tgId}` },
  ]] };

  const targetChatId = deal.createdFromChatId || env.TG_CHAT_ID;
  if (targetChatId) {
    const botToken = env.BOT_TOKEN || "";

    // Helper: send file to Telegram
    async function sendFile(method, fieldName, file, fileName, caption, replyMarkup) {
      try {
        const form = new FormData();
        form.append("chat_id", String(targetChatId));
        if (caption) { form.append("caption", caption); form.append("parse_mode", "HTML"); }
        if (replyMarkup) form.append("reply_markup", JSON.stringify(replyMarkup));
        form.append(fieldName, file, fileName || (fieldName === "photo" ? "screenshot.jpg" : "receipt.pdf"));
        const res = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, { method: "POST", body: form });
        const r = await res.json().catch(() => ({}));
        return r.ok === true;
      } catch { return false; }
    }

    // 1. Send screenshot (photo)
    if (photoFile) {
      const photoName = (photoFile instanceof File ? photoFile.name : null) || "screenshot.jpg";
      const photoOk = await sendFile("sendPhoto", "photo", photoFile, photoName, `📷 Скриншот оплаты — BUY ${shortId}`, null);
      if (!photoOk) {
        // fallback as document
        await sendFile("sendDocument", "document", photoFile, photoName, `📷 Скриншот оплаты — BUY ${shortId}`, null);
      }
    }

    // 2. Send PDF
    if (pdfFile) {
      const pdfName = (pdfFile instanceof File ? pdfFile.name : null) || "receipt.pdf";
      await sendFile("sendDocument", "document", pdfFile, pdfName, `📄 PDF чек — BUY ${shortId}`, null);
    }

    // 3. Send summary with ✅/❌ buttons (appears AFTER the files)
    await tgSend(env, targetChatId, summaryCaption, { reply_markup: kb }).catch(() => {});
  }

  return json({ ok: true, message: "Чек отправлен администратору" });
}

// Webhook from crossflag: a MW buy request got a matching offer
async function handleMwMatchNotify(request, env) {
  // Validate with a shared secret (BOT_TOKEN prefix used as cheap secret)
  const secret = request.headers.get("X-MW-Secret") || "";
  const expectedSecret = (env.BOT_TOKEN || "").slice(0, 20);
  if (!expectedSecret || secret !== expectedSecret) return bad("Unauthorized", 401);

  const body = await request.json().catch(() => ({}));
  const tgId = String(body.tgId || "").trim();
  const offer = body.offer || {};
  const reqId = String(body.reqId || "").trim();
  if (!tgId || !offer.id) return bad("Missing tgId or offer");

  // Notify via Telegram
  const r = Number(offer.rate || 0);
  const usdt = r > 0 ? (Number(offer.amountRub) / r).toFixed(2) : "—";
  const msg =
    `🎯 <b>Найдена заявка на покупку USDT!</b>\n\n` +
    `💰 Сумма: <b>${fmtRubTg(offer.amountRub)} ₽</b>\n` +
    `📊 Курс: <b>${r > 0 ? r.toFixed(2) + " ₽/USDT" : "—"}</b>\n` +
    `💵 Получите: <b>≈ ${usdt} USDT</b>\n\n` +
    `Откройте Moon Wallet → Обменять и нажмите <b>«Перейти к сделке»</b> ⚡`;
  try { await tgSend(env, tgId, msg); } catch {}

  // Mark notified in KV if reqId provided
  if (reqId) {
    const rec = await kvGet(env, `buyreq:${tgId}:${reqId}`);
    if (rec && rec.status === "PENDING" && !rec.notifiedMatchOfferId) {
      rec.notifiedMatchOfferId = offer.id;
      rec.notifiedMatchAt = now();
      await kvPut(env, `buyreq:${tgId}:${reqId}`, rec);
    }
  }

  return json({ ok: true });
}

// ── Deal complete webhook — called by crossflag on admin SUCCESS ─────────
// Credits USDT to the user's MW balance and marks the buy request as DONE.
async function handleMwDealComplete(request, env) {
  // Validate with shared secret.
  // CF_BOT_TOKEN env var in MW should be set to crossflag's BOT_TOKEN value.
  // If CF_BOT_TOKEN is not configured, we skip strict auth (idempotency + user existence still protect).
  const secret = request.headers.get("X-MW-Secret") || "";
  const cfSecret = String(env.CF_BOT_TOKEN || "").slice(0, 20);
  if (cfSecret && secret !== cfSecret) {
    return bad("Unauthorized", 401);
  }

  const body = await request.json().catch(() => ({}));
  const tgId  = String(body.tgId  || "").trim();
  const offerId = String(body.offerId || "").trim();
  if (!tgId || !offerId) return bad("Missing tgId or offerId");

  const amountRub = Number(body.amountRub || 0);
  const rate      = Number(body.rate      || 0);
  // Use explicit amountUsdt if provided, otherwise calculate
  let amountUsdt = Number(body.amountUsdt || 0);
  if (!amountUsdt && amountRub > 0 && rate > 0) amountUsdt = amountRub / rate;
  if (!amountUsdt || !isFinite(amountUsdt) || amountUsdt <= 0) {
    return bad("Cannot determine USDT amount");
  }
  const amountMicro = Math.trunc(amountUsdt * 1e6);

  // Idempotency guard — skip if already processed
  const doneKey = `mwdeal:done:${offerId}`;
  const alreadyDone = await kvGet(env, doneKey);
  if (alreadyDone) return json({ ok: true, skipped: true });

  // Credit balance
  let user;
  try {
    user = await getUser(env, tgId);
    if (!user) {
      // Notify admin for debugging
      try { await tgNotifyAdmins(env, `⚠️ mw_deal_complete: user not found tgId=${tgId} offerId=${offerId}`); } catch {}
      return bad("User not found", 404);
    }
    await adjustBalance(env, tgId, amountMicro, `P2P buy complete: ${offerId}`);
    await kvPut(env, doneKey, { ts: now(), tgId, offerId, amountMicro }, { expirationTtl: 3600 * 24 * 180 });
    // Notify admin that webhook was processed successfully
    try { await tgNotifyAdmins(env, `✅ MW deal complete: tgId=${tgId} +${(amountMicro/1e6).toFixed(2)} USDT (offer ${offerId})`); } catch {}
  } catch (e) {
    try { await tgNotifyAdmins(env, `❌ mw_deal_complete error: tgId=${tgId} err=${e.message}`); } catch {}
    return bad("Balance error: " + (e.message || String(e)), 500);
  }

  // Mark the matching buy request as COMPLETED
  const mwReqId = String(body.mwReqId || "").trim();
  if (mwReqId) {
    try {
      const rec = await kvGet(env, `buyreq:${tgId}:${mwReqId}`);
      if (rec && rec.status !== "COMPLETED") {
        rec.status = "COMPLETED";
        rec.completedAt = now();
        rec.completedOfferId = offerId;
        rec.creditedMicro = amountMicro;
        await kvPut(env, `buyreq:${tgId}:${mwReqId}`, rec);
      }
    } catch {}
  } else {
    // If mwReqId not passed, search all PENDING requests for this user
    try {
      const idx = (await kvGet(env, `buyreq_idx:${tgId}`)) || [];
      for (const reqId of idx) {
        const rec = await kvGet(env, `buyreq:${tgId}:${reqId}`);
        if (rec && (rec.status === "PENDING" || rec.status === "PAID")) {
          rec.status = "COMPLETED";
          rec.completedAt = now();
          rec.completedOfferId = offerId;
          rec.creditedMicro = amountMicro;
          await kvPut(env, `buyreq:${tgId}:${reqId}`, rec);
          break; // mark only the first matching open request
        }
      }
    } catch {}
  }

  // Update deal record in KV so the frontend poll sees SUCCESS
  try {
    const dealKey = `exch:${tgId}:${offerId}`;
    const deal = (await kvGet(env, dealKey)) || {};
    deal.status = "SUCCESS";
    deal.completedAt = now();
    deal.creditedMicro = amountMicro;
    deal.creditedUsdt = (amountMicro / 1e6).toFixed(2);
    deal.amountUsdt = (amountMicro / 1e6).toFixed(2);
    await kvPut(env, dealKey, deal);
  } catch {}

  // Notify user via Telegram
  const usdtStr = (amountMicro / 1e6).toFixed(2);
  const rubStr  = amountRub > 0 ? `${amountRub} ₽` : "—";
  const msg =
    `✅ <b>Сделка завершена!</b>\n\n` +
    `💵 Зачислено: <b>${usdtStr} USDT</b>\n` +
    `💳 Сумма оплаты: <b>${rubStr}</b>\n\n` +
    `Средства поступили на ваш баланс Moon Wallet 💰🎉`;
  try { await tgSend(env, tgId, msg); } catch {}

  return json({ ok: true, amountMicro });
}

// ── Buy request match monitor (runs every 2 min via cron) ────
// Checks all PENDING MW buy requests against live crossflag offers.
// When a matching offer is found, notifies the user via MW Telegram bot.
async function scanBuyRequestMatches(env) {
  try {
    // Fetch live crossflag offers once
    let offers = [];
    try {
      const r = await fetch(CROSSFLAG_BASE + "/api/public/buy_offers");
      const j = await r.json().catch(() => null);
      if (j?.ok && Array.isArray(j.offers)) offers = j.offers;
    } catch { return; }
    if (!offers.length) return;

    // Iterate all buyreq index keys
    const idxKeys = await kvList(env, "buyreq_idx:");
    for (const idxKey of idxKeys) {
      const tgId = idxKey.replace("buyreq_idx:", "");
      const idx = (await kvGet(env, idxKey)) || [];
      for (const reqId of idx.slice(0, 20)) {
        const rec = await kvGet(env, `buyreq:${tgId}:${reqId}`);
        if (!rec || rec.status !== "PENDING") continue;

        // Skip expired (> 7 min)
        if (now() - Number(rec.createdAt || 0) > BUY_REQ_TTL_MS) {
          rec.status = "EXPIRED";
          await kvPut(env, `buyreq:${tgId}:${reqId}`, rec);
          continue;
        }

        // Already notified about a match?
        if (rec.notifiedMatchOfferId) continue;

        const min = Number(rec.minRub), max = Number(rec.maxRub);
        let best = null, bestScore = Infinity;
        const mid = (min + max) / 2;
        for (const o of offers) {
          const a = Number(o.amountRub);
          if (!isFinite(a) || a < min || a > max) continue;
          const score = Math.abs(a - mid);
          if (score < bestScore) { bestScore = score; best = o; }
        }
        if (!best) continue;

        // Found a match — notify user via Telegram
        const r = Number(best.rate || 0);
        const usdt = r > 0 ? (Number(best.amountRub) / r).toFixed(2) : "—";
        const msg =
          `🎯 <b>Найдена заявка на покупку USDT!</b>\n\n` +
          `Ваш запрос на сумму <b>${fmtRubTg(min)}–${fmtRubTg(max)} ₽</b> совпал с предложением в стакане:\n\n` +
          `💰 Сумма: <b>${fmtRubTg(best.amountRub)} ₽</b>\n` +
          `📊 Курс: <b>${r > 0 ? r.toFixed(2) + " ₽/USDT" : "—"}</b>\n` +
          `💵 Получите: <b>≈ ${usdt} USDT</b>\n\n` +
          `Откройте Moon Wallet → Обменять и нажмите <b>«Перейти к сделке»</b> ⚡`;

        try { await tgSend(env, tgId, msg); } catch {}

        // Mark notified so we don't spam
        rec.notifiedMatchOfferId = best.id;
        rec.notifiedMatchAt = now();
        await kvPut(env, `buyreq:${tgId}:${reqId}`, rec);
      }
    }
  } catch {}
}

function fmtRubTg(n) {
  try { return Number(n || 0).toLocaleString("ru-RU"); } catch { return String(n); }
}

export default {
  fetch: handleRequest,
  async scheduled(event, env, ctx) {
    ctx.waitUntil(scanRapiraDeposits(env));
    ctx.waitUntil(scanBuyRequestMatches(env));
  },
};
