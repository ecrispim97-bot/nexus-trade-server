// ============================================================
// NEXUS TRADE IA — Servidor Webhook + IA em Tempo Real
// ============================================================
// Deploy gratuito: Railway.app ou Render.com
// Recebe dados do TradingView (Pine Script) via Webhook
// Chama Claude IA para análise automática
// Envia resultado ao vivo via WebSocket para o dashboard
// ============================================================

const express  = require('express');
const cors     = require('cors');
const http     = require('http');
const WebSocket= require('ws');
const fetch    = require('node-fetch');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());

// ── Chave Anthropic (coloque no ambiente: ANTHROPIC_API_KEY) ──
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY || '';

// ── Token de segurança do Webhook (defina no TradingView também) ──
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || 'nexus2026';

// ── Último estado recebido ──
let ultimosDados  = null;
let ultimaAnalise = null;
let analisando    = false;

// ─────────────────────────────────────────────
// WebSocket — broadcast para todos os clientes
// ─────────────────────────────────────────────
function broadcast(tipo, payload) {
  const msg = JSON.stringify({ tipo, payload, ts: Date.now() });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  });
}

wss.on('connection', ws => {
  console.log('[WS] Cliente conectado. Total:', wss.clients.size);
  // Envia último estado ao novo cliente
  if (ultimosDados)  ws.send(JSON.stringify({ tipo: 'dados',   payload: ultimosDados,  ts: Date.now() }));
  if (ultimaAnalise) ws.send(JSON.stringify({ tipo: 'analise', payload: ultimaAnalise, ts: Date.now() }));
  ws.on('close', () => console.log('[WS] Cliente desconectado. Total:', wss.clients.size));
});

// ─────────────────────────────────────────────
// POST /webhook — recebe dados do TradingView
// ─────────────────────────────────────────────
app.post('/webhook', async (req, res) => {
  const { token, ...dados } = req.body;

  // Validação do token
  if (token !== WEBHOOK_TOKEN) {
    console.warn('[WEBHOOK] Token inválido:', token);
    return res.status(401).json({ erro: 'Token inválido' });
  }

  console.log('[WEBHOOK] Dados recebidos:', JSON.stringify(dados).slice(0, 200));

  ultimosDados = { ...dados, recebido_em: new Date().toISOString() };

  // Broadcast imediato dos dados brutos
  broadcast('dados', ultimosDados);

  res.json({ ok: true, ts: Date.now() });

  // Dispara análise IA em background
  if (!analisando) analisarComIA(ultimosDados);
});

// ─────────────────────────────────────────────
// Análise com Claude IA
// ─────────────────────────────────────────────
async function analisarComIA(dados) {
  if (analisando || !ANTHROPIC_KEY) return;
  analisando = true;
  broadcast('status', { msg: 'IA analisando...', cor: 'amarelo' });

  const prompt = `Você é um analista técnico sênior especializado em XAUUSD. Analise os dados em tempo real abaixo e gere uma análise técnica PROFISSIONAL e OBJETIVA. NÃO mencione robôs ou estratégias específicas — apenas análise técnica pura.

DADOS EM TEMPO REAL (TradingView Webhook):
${JSON.stringify(dados, null, 2)}

CONTEXTO DOS INDICADORES:
- Vol_Ticks: Volume em ticks da vela atual
- Vol_Delta: Diferença entre volume comprador e vendedor
- Vol_POC: Point of Control do Volume Profile
- Vol_VAH/VAL: Value Area High/Low (70% do volume)
- FVG_Bullish/Bearish: Fair Value Gaps (desequilíbrios de preço)
- IFVG: Inverse Fair Value Gap
- DiNapoli_K / DiNapoli_D: Linhas do oscilador DiNapoli (MACD/Stoch)
- NearBand: Distância da banda do oscilador
- Oscillator_Status: Estado atual (K2, D2 = cruzamentos)
- Liq_Levels: Níveis de liquidez mapeados (pools de stops)
- Session: Sessão atual (NY, Londres, Ásia)
- Preco: Preço atual do XAUUSD
- High_24h / Low_24h: Máxima e mínima das últimas 24h

Responda APENAS com JSON puro sem markdown:
{
  "vies": "ALTISTA ou BAIXISTA ou NEUTRO",
  "vies_forca": 0-100,
  "confianca": 0-100,
  "sinal": "COMPRA ou VENDA ou AGUARDAR",
  "urgencia": "IMEDIATA ou PROXIMA_VELA ou AGUARDAR",
  "preco_entrada": número,
  "tp1": número,
  "tp2": número,
  "tp3": número,
  "sl": número,
  "volume": {
    "leitura": "string",
    "delta_bias": "COMPRADOR ou VENDEDOR ou NEUTRO",
    "poc_vs_preco": "ACIMA ou ABAIXO ou NO_POC",
    "value_area": "string"
  },
  "liquidez": {
    "pool_mais_proximo_acima": número,
    "pool_mais_proximo_abaixo": número,
    "alvo_liquidez_principal": número,
    "descricao": "string"
  },
  "fvg": {
    "bullish_ativo": true ou false,
    "bearish_ativo": true ou false,
    "ifvg_relevante": "string",
    "descricao": "string"
  },
  "dinapoli": {
    "k": número,
    "d": número,
    "nearband": número,
    "fundo": número,
    "topo": número,
    "inversao": número,
    "sinal": "SOBRECOMPRADO ou SOBREVENDIDO ou CRUZAMENTO_ALTA ou CRUZAMENTO_BAIXA ou NEUTRO",
    "leitura": "string"
  },
  "estrutura": {
    "tendencia_macro": "string",
    "swing_atual": "string",
    "choch": "SIM ou NAO",
    "order_block": "string"
  },
  "narrativa": "3-5 frases técnicas cobrindo volume, liquidez, FVG e osciladores",
  "alerta": "string — mensagem de alerta se houver setup imediato, vazio se não houver",
  "nivel_risco": "BAIXO ou MEDIO ou ALTO"
}`;

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 1500,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const d = await res.json();
    if (d.error) throw new Error(d.error.message);

    const raw    = d.content?.find(b => b.type === 'text')?.text || '{}';
    const result = JSON.parse(raw.replace(/```json|```/g, '').trim());

    ultimaAnalise = { ...result, analisado_em: new Date().toISOString(), dados_base: dados };

    console.log('[IA] Análise:', result.vies, result.sinal, result.confianca + '%');

    // Broadcast da análise
    broadcast('analise', ultimaAnalise);

    // Broadcast de alerta se urgente
    if (result.alerta && result.urgencia === 'IMEDIATA') {
      broadcast('alerta', {
        tipo: result.sinal,
        mensagem: result.alerta,
        entrada: result.preco_entrada,
        tp1: result.tp1,
        sl: result.sl,
        confianca: result.confianca
      });
    }

    broadcast('status', { msg: `Análise concluída — ${result.vies} ${result.confianca}%`, cor: 'verde' });

  } catch (e) {
    console.error('[IA] Erro:', e.message);
    broadcast('status', { msg: 'Erro na análise: ' + e.message, cor: 'vermelho' });
  }

  analisando = false;
}

// ─────────────────────────────────────────────
// GET /status — health check
// ─────────────────────────────────────────────
app.get('/status', (req, res) => {
  res.json({
    ok: true,
    clientes_ws: wss.clients.size,
    ultimo_dado: ultimosDados?.recebido_em || null,
    ultima_analise: ultimaAnalise?.analisado_em || null,
    anthropic_key: ANTHROPIC_KEY ? 'configurada' : 'FALTANDO'
  });
});

// ─────────────────────────────────────────────
// GET /ultimo — retorna últimos dados e análise
// ─────────────────────────────────────────────
app.get('/ultimo', (req, res) => {
  res.json({ dados: ultimosDados, analise: ultimaAnalise });
});

// ─────────────────────────────────────────────
// Iniciar servidor
// ─────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🚀 Nexus Trade IA — Servidor rodando na porta ${PORT}`);
  console.log(`📡 Webhook: POST /webhook`);
  console.log(`🔌 WebSocket: ws://localhost:${PORT}`);
  console.log(`✅ Status: GET /status`);
  console.log(`🔑 Anthropic Key: ${ANTHROPIC_KEY ? 'OK' : 'FALTANDO — configure ANTHROPIC_API_KEY'}`);
  console.log(`🔐 Webhook Token: ${WEBHOOK_TOKEN}\n`);
});
