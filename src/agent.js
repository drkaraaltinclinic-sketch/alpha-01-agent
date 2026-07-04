/**
 * ALPHA-01 — Signal Generation Agent
 * Subscribes to GECKO-01 event bus.
 * Runs full technical analysis on every price tick.
 * Outputs trade recommendations with entry, stop-loss, and targets.
 */

const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const cors = require('cors');
const path = require('path');

// ─── Config ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
const GECKO_URL = process.env.GECKO_URL || 'wss://gecko-01-agent-production.up.railway.app/?agent=ALPHA-01';
const SIGNAL_CONFIDENCE_THRESHOLD = parseFloat(process.env.SIGNAL_CONFIDENCE_THRESHOLD || '55');

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
  startTime:      Date.now(),
  signalCount:    0,
  tickCount:      0,
  alertCount:     0,
  geckoConnected: false,
  lastSignals:    {},   // latest signal per asset
  priceHistory:   {},   // rolling price history per asset
  signals:        [],   // signal log (last 100)
  errors:         [],
};

// ─── Technical Analysis Library ──────────────────────────────────────────────

/**
 * RSI — Relative Strength Index
 * Standard 14-period RSI
 */
function calcRSI(prices, period = 14) {
  if (prices.length < period + 1) return null;
  const deltas = prices.slice(1).map((p, i) => p - prices[i]);
  let gains = 0, losses = 0;
  deltas.slice(0, period).forEach(d => {
    if (d > 0) gains += d;
    else losses += Math.abs(d);
  });
  let avgGain = gains / period;
  let avgLoss = losses / period;
  for (let i = period; i < deltas.length; i++) {
    const d = deltas[i];
    avgGain = (avgGain * (period - 1) + Math.max(d, 0)) / period;
    avgLoss = (avgLoss * (period - 1) + Math.max(-d, 0)) / period;
  }
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

/**
 * EMA — Exponential Moving Average
 */
function calcEMA(prices, period) {
  if (prices.length < period) return null;
  const k = 2 / (period + 1);
  let ema = prices.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < prices.length; i++) {
    ema = prices[i] * k + ema * (1 - k);
  }
  return ema;
}

/**
 * MACD — Moving Average Convergence Divergence
 * Standard 12/26/9
 */
function calcMACD(prices) {
  if (prices.length < 26) return null;
  const ema12 = calcEMA(prices, 12);
  const ema26 = calcEMA(prices, 26);
  if (ema12 === null || ema26 === null) return null;
  const macdLine = ema12 - ema26;

  // Signal line: 9-period EMA of MACD
  // Approximate by using recent MACD values
  const macdValues = [];
  for (let i = 26; i <= prices.length; i++) {
    const slice = prices.slice(0, i);
    const e12 = calcEMA(slice, 12);
    const e26 = calcEMA(slice, 26);
    if (e12 && e26) macdValues.push(e12 - e26);
  }
  const signalLine = macdValues.length >= 9 ? calcEMA(macdValues, 9) : macdLine;
  const histogram = macdLine - (signalLine || macdLine);

  return { macdLine, signalLine, histogram };
}

/**
 * Bollinger Bands — 20-period, 2 standard deviations
 */
function calcBollinger(prices, period = 20) {
  if (prices.length < period) return null;
  const slice = prices.slice(-period);
  const mean = slice.reduce((a, b) => a + b, 0) / period;
  const variance = slice.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / period;
  const std = Math.sqrt(variance);
  return {
    upper: mean + 2 * std,
    middle: mean,
    lower: mean - 2 * std,
    bandwidth: (4 * std) / mean,
    percentB: (prices[prices.length - 1] - (mean - 2 * std)) / (4 * std),
  };
}

/**
 * Volume analysis — compare recent volume to average
 */
function calcVolumeSignal(volumes) {
  if (volumes.length < 5) return null;
  const recent = volumes[volumes.length - 1];
  const avg = volumes.slice(-10).reduce((a, b) => a + b, 0) / Math.min(volumes.length, 10);
  const ratio = recent / avg;
  return {
    ratio,
    spike: ratio > 2.0,
    elevated: ratio > 1.5,
    low: ratio < 0.5,
    label: ratio > 2.0 ? 'SPIKE' : ratio > 1.5 ? 'ELEVATED' : ratio < 0.5 ? 'LOW' : 'NORMAL',
  };
}

/**
 * Momentum — rate of change over N periods
 */
function calcMomentum(prices, period = 10) {
  if (prices.length < period + 1) return null;
  const current = prices[prices.length - 1];
  const past = prices[prices.length - 1 - period];
  return ((current - past) / past) * 100;
}

/**
 * Support & Resistance — simple swing high/low detection
 */
function calcSupportResistance(prices) {
  if (prices.length < 10) return null;
  const recent = prices.slice(-20);
  const high = Math.max(...recent);
  const low = Math.min(...recent);
  const current = prices[prices.length - 1];
  return {
    resistance: high,
    support: low,
    distanceToResistance: ((high - current) / current) * 100,
    distanceToSupport: ((current - low) / current) * 100,
    position: (current - low) / (high - low), // 0=at support, 1=at resistance
  };
}

// ─── Signal Engine ────────────────────────────────────────────────────────────

function generateSignal(asset, tick) {
  const prices = state.priceHistory[asset]?.prices || [];
  const volumes = state.priceHistory[asset]?.volumes || [];

  if (prices.length < 5) return null;

  const current = prices[prices.length - 1];

  // Run all indicators
  const rsi        = calcRSI(prices);
  const macd       = calcMACD(prices);
  const bollinger  = calcBollinger(prices);
  const volume     = calcVolumeSignal(volumes);
  const momentum   = calcMomentum(prices);
  const sr         = calcSupportResistance(prices);
  const ema9       = calcEMA(prices, 9);
  const ema21      = calcEMA(prices, 21);
  const ema50      = calcEMA(prices, 50);

  // ── Scoring system ──
  // Each indicator votes: +points = bullish, -points = bearish
  let score = 0;
  let maxScore = 0;
  const reasons = [];
  const warnings = [];

  // RSI (weight: 25)
  if (rsi !== null) {
    maxScore += 25;
    if (rsi < 30) {
      score += 25;
      reasons.push(`RSI oversold (${rsi.toFixed(1)}) — reversal likely`);
    } else if (rsi < 40) {
      score += 15;
      reasons.push(`RSI approaching oversold (${rsi.toFixed(1)})`);
    } else if (rsi > 70) {
      score -= 25;
      warnings.push(`RSI overbought (${rsi.toFixed(1)}) — correction risk`);
    } else if (rsi > 60) {
      score -= 10;
      warnings.push(`RSI elevated (${rsi.toFixed(1)})`);
    } else {
      score += 5;
      reasons.push(`RSI neutral (${rsi.toFixed(1)})`);
    }
  }

  // MACD (weight: 20)
  if (macd !== null) {
    maxScore += 20;
    if (macd.histogram > 0 && macd.macdLine > 0) {
      score += 20;
      reasons.push(`MACD bullish crossover — histogram ${macd.histogram.toFixed(4)}`);
    } else if (macd.histogram > 0) {
      score += 10;
      reasons.push(`MACD histogram positive`);
    } else if (macd.histogram < 0 && macd.macdLine < 0) {
      score -= 20;
      warnings.push(`MACD bearish — histogram ${macd.histogram.toFixed(4)}`);
    } else {
      score -= 8;
      warnings.push(`MACD histogram negative`);
    }
  }

  // Bollinger Bands (weight: 20)
  if (bollinger !== null) {
    maxScore += 20;
    if (bollinger.percentB < 0.05) {
      score += 20;
      reasons.push(`Price at lower Bollinger band — mean reversion signal`);
    } else if (bollinger.percentB < 0.2) {
      score += 12;
      reasons.push(`Price near lower Bollinger band`);
    } else if (bollinger.percentB > 0.95) {
      score -= 20;
      warnings.push(`Price at upper Bollinger band — overextended`);
    } else if (bollinger.percentB > 0.8) {
      score -= 10;
      warnings.push(`Price near upper Bollinger band`);
    } else {
      score += 5;
    }
  }

  // EMA trend (weight: 20)
  if (ema9 && ema21) {
    maxScore += 20;
    if (ema9 > ema21) {
      score += ema50 && ema21 > ema50 ? 20 : 12;
      reasons.push(`EMA bullish alignment — 9 > 21${ema50 && ema21 > ema50 ? ' > 50' : ''}`);
    } else {
      score -= ema50 && ema21 < ema50 ? 20 : 12;
      warnings.push(`EMA bearish alignment — 9 < 21${ema50 && ema21 < ema50 ? ' < 50' : ''}`);
    }
  }

  // Momentum (weight: 10)
  if (momentum !== null) {
    maxScore += 10;
    if (momentum > 5) {
      score += 10;
      reasons.push(`Strong positive momentum +${momentum.toFixed(2)}%`);
    } else if (momentum > 0) {
      score += 5;
      reasons.push(`Positive momentum +${momentum.toFixed(2)}%`);
    } else if (momentum < -5) {
      score -= 10;
      warnings.push(`Strong negative momentum ${momentum.toFixed(2)}%`);
    } else {
      score -= 5;
    }
  }

  // Volume (weight: 5)
  if (volume !== null) {
    maxScore += 5;
    if (volume.spike || volume.elevated) {
      score += 5;
      reasons.push(`Volume ${volume.label} (${volume.ratio.toFixed(2)}x avg) — confirms move`);
    } else if (volume.low) {
      warnings.push(`Low volume (${volume.ratio.toFixed(2)}x avg) — weak conviction`);
    }
  }

  // 24h change from GECKO alert
  if (tick.change_24h) {
    const chg = tick.change_24h;
    if (chg < -10) {
      score += 8; // deep dip = opportunity
      reasons.push(`Deep correction ${chg.toFixed(2)}% — potential entry`);
    } else if (chg > 15) {
      score -= 8; // already pumped
      warnings.push(`Already up ${chg.toFixed(2)}% — chasing risk`);
    }
  }

  // ── Normalize score to confidence 0-100 ──
  const normalizedScore = maxScore > 0 ? ((score + maxScore) / (2 * maxScore)) * 100 : 50;
  const confidence = Math.min(100, Math.max(0, normalizedScore));

  // ── Direction ──
  let direction, label;
  if (confidence >= 65) {
    direction = 'LONG';
    label = confidence >= 80 ? 'STRONG BUY' : 'BUY';
  } else if (confidence <= 35) {
    direction = 'SHORT';
    label = confidence <= 20 ? 'STRONG SELL' : 'SELL';
  } else {
    direction = 'NEUTRAL';
    label = 'HOLD';
  }

  // ── Trade Levels ──
  const atr = calcATR(prices);
  const atrMultiplier = 1.5;

  let entry, stopLoss, target1, target2, target3, riskReward;

  if (direction === 'LONG') {
    entry    = current;
    stopLoss = sr ? Math.min(current - atr * atrMultiplier, sr.support * 0.99) : current - atr * atrMultiplier;
    target1  = current + atr * 1.5;
    target2  = current + atr * 3.0;
    target3  = sr ? Math.max(current + atr * 4.5, sr.resistance * 0.99) : current + atr * 4.5;
    riskReward = (target2 - entry) / (entry - stopLoss);
  } else if (direction === 'SHORT') {
    entry    = current;
    stopLoss = sr ? Math.max(current + atr * atrMultiplier, sr.resistance * 1.01) : current + atr * atrMultiplier;
    target1  = current - atr * 1.5;
    target2  = current - atr * 3.0;
    target3  = sr ? Math.min(current - atr * 4.5, sr.support * 1.01) : current - atr * 4.5;
    riskReward = (entry - target2) / (stopLoss - entry);
  } else {
    entry = stopLoss = target1 = target2 = target3 = current;
    riskReward = 0;
  }

  // ── Position sizing (% of portfolio, Kelly-lite) ──
  const kellyFraction = confidence > 50 ? ((confidence / 100 - 0.5) * 2) * 0.25 : 0;
  const positionSize = Math.min(kellyFraction * 100, 20); // cap at 20% of portfolio

  return {
    asset,
    symbol:       tick.symbol?.toUpperCase() || asset,
    direction,
    label,
    confidence:   parseFloat(confidence.toFixed(1)),
    positionSize: parseFloat(positionSize.toFixed(1)),
    entry:        parseFloat(entry.toFixed(6)),
    stopLoss:     parseFloat(stopLoss.toFixed(6)),
    target1:      parseFloat(target1.toFixed(6)),
    target2:      parseFloat(target2.toFixed(6)),
    target3:      parseFloat(target3.toFixed(6)),
    riskReward:   parseFloat((riskReward || 0).toFixed(2)),
    indicators: {
      rsi:       rsi ? parseFloat(rsi.toFixed(2)) : null,
      macd:      macd ? { line: parseFloat(macd.macdLine.toFixed(6)), histogram: parseFloat(macd.histogram.toFixed(6)) } : null,
      bollinger: bollinger ? { upper: parseFloat(bollinger.upper.toFixed(4)), lower: parseFloat(bollinger.lower.toFixed(4)), percentB: parseFloat(bollinger.percentB.toFixed(3)) } : null,
      ema9:      ema9 ? parseFloat(ema9.toFixed(4)) : null,
      ema21:     ema21 ? parseFloat(ema21.toFixed(4)) : null,
      momentum:  momentum ? parseFloat(momentum.toFixed(2)) : null,
      volume:    volume || null,
    },
    reasons,
    warnings,
    change_24h: tick.change_24h,
    price:      current,
    timestamp:  new Date().toISOString(),
  };
}

/**
 * ATR — Average True Range (approximated from price history)
 */
function calcATR(prices, period = 14) {
  if (prices.length < 2) return prices[0] * 0.02;
  const trs = prices.slice(1).map((p, i) => Math.abs(p - prices[i]));
  const recent = trs.slice(-period);
  return recent.reduce((a, b) => a + b, 0) / recent.length;
}

// ─── App Setup ───────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ─── ALPHA-01 Event Bus (broadcasts to downstream agents) ────────────────────
function broadcast(event) {
  const payload = JSON.stringify({ ...event, agentId: 'ALPHA-01', timestamp: new Date().toISOString() });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  });
}

function emit(type, topic, data, severity = 'INFO') {
  broadcast({ type, topic, data, severity });
  console.log(`[${new Date().toISOString()}] [${type}] [${topic}] ${JSON.stringify(data).substring(0, 140)}`);
}

// ─── Price History Management ─────────────────────────────────────────────────
function updateHistory(id, price, volume) {
  if (!state.priceHistory[id]) state.priceHistory[id] = { prices: [], volumes: [] };
  state.priceHistory[id].prices.push(price);
  state.priceHistory[id].volumes.push(volume || 0);
  // Keep last 100 data points
  if (state.priceHistory[id].prices.length > 100) state.priceHistory[id].prices.shift();
  if (state.priceHistory[id].volumes.length > 100) state.priceHistory[id].volumes.shift();
}

// ─── Handle Incoming GECKO Events ────────────────────────────────────────────
function handleGeckoEvent(event) {
  const { topic, data } = event;

  if (topic === 'gecko.market.tick' || topic === 'gecko.rwa.tick') {
    state.tickCount++;
    updateHistory(data.id, data.price, data.volume);

    const signal = generateSignal(data.id, data);
    if (!signal) return;

    state.lastSignals[data.id] = signal;

    // Only broadcast signals above confidence threshold
    if (signal.confidence >= SIGNAL_CONFIDENCE_THRESHOLD || signal.confidence <= (100 - SIGNAL_CONFIDENCE_THRESHOLD)) {
      state.signalCount++;
      state.signals.unshift(signal);
      if (state.signals.length > 100) state.signals.pop();

      emit('SIGNAL', 'alpha.signal', signal, signal.direction === 'NEUTRAL' ? 'INFO' : 'HIGH');
    }

    // Also emit raw indicator data for dashboard
    emit('INDICATORS', 'alpha.indicators', {
      asset:      data.id,
      symbol:     data.symbol,
      price:      data.price,
      indicators: signal.indicators,
      timestamp:  new Date().toISOString(),
    });
  }

  if (topic === 'gecko.alert.fire') {
    state.alertCount++;
    // Generate enhanced alert signal
    const tick = { ...data, price: data.price || 0, change_24h: data.value || 0 };
    const assetId = Object.keys(state.priceHistory).find(id => state.priceHistory[id] && data.asset?.toLowerCase().includes(id.split('-')[0]));

    emit('ALERT', 'alpha.alert', {
      type:      data.type,
      severity:  data.severity,
      asset:     data.asset,
      price:     data.price,
      value:     data.value,
      action:    data.type === 'PUMP' ? 'MONITOR FOR ENTRY ON PULLBACK' :
                 data.type === 'DUMP' ? 'MONITOR FOR REVERSAL SIGNAL' :
                 data.type === 'VOL_SPIKE' ? 'CONFIRM DIRECTION BEFORE ENTRY' :
                 data.type === 'ATH_NEAR' ? 'TAKE PROFIT / TIGHTEN STOP' : 'MONITOR',
      timestamp: new Date().toISOString(),
    }, data.severity);
  }

  if (topic === 'gecko.cycle.complete') {
    emit('SYS', 'alpha.cycle.complete', {
      ticksProcessed: state.tickCount,
      signalsGenerated: state.signalCount,
      activeSignals: Object.keys(state.lastSignals).length,
    });
  }

  if (topic === 'gecko.handshake') {
    console.log(`✓ GECKO-01 handshake received — ${Object.keys(data.snapshot || {}).length} assets in snapshot`);
    // Seed price history from snapshot
    Object.values(data.snapshot || {}).forEach(coin => {
      if (coin.sparkline && coin.sparkline.length) {
        state.priceHistory[coin.id] = {
          prices: coin.sparkline.slice(-50),
          volumes: new Array(Math.min(coin.sparkline.length, 50)).fill(coin.volume || 0),
        };
      }
    });
    emit('SYS', 'alpha.ready', { message: 'ALPHA-01 seeded from GECKO snapshot — signal engine active' });
  }
}

// ─── GECKO-01 WebSocket Connection ────────────────────────────────────────────
let geckoWs = null;
let geckoReconnectTimer = null;

function connectToGecko() {
  console.log(`Connecting to GECKO-01 at ${GECKO_URL}...`);
  geckoWs = new WebSocket(GECKO_URL);

  geckoWs.on('open', () => {
    state.geckoConnected = true;
    console.log('✓ Connected to GECKO-01 event bus');
    emit('SYS', 'alpha.gecko.connected', { url: GECKO_URL });
    clearTimeout(geckoReconnectTimer);
    // Subscribe to relevant topics
    geckoWs.send(JSON.stringify({ type: 'SUBSCRIBE', topics: ['gecko.market.tick', 'gecko.rwa.tick', 'gecko.alert.fire', 'gecko.cycle.complete'] }));
  });

  geckoWs.on('message', (raw) => {
    try {
      const event = JSON.parse(raw.toString());
      handleGeckoEvent(event);
    } catch (e) {
      console.warn('Parse error:', e.message);
    }
  });

  geckoWs.on('close', () => {
    state.geckoConnected = false;
    console.warn('✗ GECKO-01 disconnected — reconnecting in 5s...');
    emit('SYS', 'alpha.gecko.disconnected', { reconnectIn: 5000 });
    geckoReconnectTimer = setTimeout(connectToGecko, 5000);
  });

  geckoWs.on('error', (err) => {
    console.error('GECKO WS error:', err.message);
    state.errors.push({ time: new Date().toISOString(), message: err.message });
  });

  // Keepalive ping every 20s
  setInterval(() => {
    if (geckoWs?.readyState === WebSocket.OPEN) {
      geckoWs.send(JSON.stringify({ type: 'PING' }));
    }
  }, 20000);
}

// ─── Dashboard WebSocket (for browser clients) ────────────────────────────────
wss.on('connection', (ws, req) => {
  const clientId = req.url?.replace('/?agent=', '') || `CLIENT-${Date.now()}`;
  console.log(`[WS] Client connected: ${clientId}`);

  // Send current state snapshot on connect
  ws.send(JSON.stringify({
    type: 'SYS', topic: 'alpha.handshake', agentId: 'ALPHA-01',
    timestamp: new Date().toISOString(),
    data: {
      message: 'Connected to ALPHA-01 Signal Agent',
      geckoConnected: state.geckoConnected,
      signalCount: state.signalCount,
      lastSignals: state.lastSignals,
      recentSignals: state.signals.slice(0, 20),
      stats: {
        uptime: Date.now() - state.startTime,
        tickCount: state.tickCount,
        signalCount: state.signalCount,
        alertCount: state.alertCount,
      },
    },
  }));

  ws.on('close', () => console.log(`[WS] Client disconnected: ${clientId}`));
  ws.on('error', (e) => console.error(`[WS] Error ${clientId}:`, e.message));
});

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/health', (_, res) => res.json({
  agent: 'ALPHA-01', status: 'LIVE',
  geckoConnected: state.geckoConnected,
  uptime: Date.now() - state.startTime,
  signalCount: state.signalCount,
  tickCount: state.tickCount,
}));

app.get('/signals', (_, res) => res.json({
  agent: 'ALPHA-01',
  timestamp: new Date().toISOString(),
  signals: state.signals.slice(0, 50),
}));

app.get('/signals/latest', (_, res) => res.json({
  agent: 'ALPHA-01',
  timestamp: new Date().toISOString(),
  signals: state.lastSignals,
}));

app.get('/signals/:asset', (req, res) => {
  const signal = state.lastSignals[req.params.asset];
  if (!signal) return res.status(404).json({ error: 'No signal for asset' });
  res.json(signal);
});

// ─── Boot ─────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log('');
  console.log('╔════════════════════════════════════════════════╗');
  console.log('║     ALPHA-01 Signal Generation Agent           ║');
  console.log('║     Sub-Agent of GECKO-01 · v1.0.0             ║');
  console.log('╚════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  HTTP  →  http://localhost:${PORT}`);
  console.log(`  WS    →  ws://localhost:${PORT}`);
  console.log(`  GECKO →  ${GECKO_URL}`);
  console.log(`  Threshold → ${SIGNAL_CONFIDENCE_THRESHOLD}% confidence`);
  console.log('');

  connectToGecko();
});

process.on('SIGTERM', () => { console.log('ALPHA-01 shutting down...'); process.exit(0); });
process.on('SIGINT',  () => { console.log('ALPHA-01 shutting down...'); process.exit(0); });
