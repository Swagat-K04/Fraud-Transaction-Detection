import { useState, useEffect, useRef } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine
} from 'recharts'

const BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000'

const TOOLTIP_STYLE = {
  backgroundColor: '#111827',
  border: '1px solid #374151',
  borderRadius: 8,
  fontFamily: "'JetBrains Mono', monospace",
  fontSize: 11,
  color: '#f3f4f6',
}

function MetricPill({ label, value, color }) {
  return (
    <div className="bg-gray-800/60 border border-gray-700/50 rounded-lg p-3 text-center">
      <p className="text-[10px] font-mono uppercase tracking-widest text-gray-500 mb-1">{label}</p>
      <p className={`text-xl font-mono font-bold ${color}`}>{value}</p>
    </div>
  )
}

export function ThresholdPanel({ activeThreshold, onThresholdChange }) {
  // draft lives here and is NEVER overwritten by polling —
  // only the user moving the slider changes it
  const [draft,    setDraft]   = useState(activeThreshold ?? 0.5)
  const [stats,    setStats]   = useState(null)
  const [curve,    setCurve]   = useState([])
  const [saving,   setSaving]  = useState(false)
  const [saveMsg,  setSaveMsg] = useState(null)   // null | 'ok' | 'error'
  const initialised = useRef(false)

  // Sync draft ONLY on first mount so slider position survives tab switches
  useEffect(() => {
    if (!initialised.current) {
      setDraft(activeThreshold)
      initialised.current = true
    }
  }, [activeThreshold])

  // Poll stats (never touch draft)
  useEffect(() => {
    const loadStats = () => {
      fetch(BASE + '/api/threshold')
        .then(r => r.json())
        .then(d => setStats(d))
        .catch(() => {})

      fetch(BASE + '/api/threshold/curve')
        .then(r => r.json())
        .then(d => setCurve(d.curve || []))
        .catch(() => {})
    }
    loadStats()
    const t = setInterval(loadStats, 10000)
    return () => clearInterval(t)
  }, [])

  const applyThreshold = async () => {
    setSaving(true)
    setSaveMsg(null)
    try {
      const res = await fetch(BASE + '/api/threshold', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ threshold: draft }),
      })
      if (res.ok) {
        const data = await res.json()
        onThresholdChange(data.threshold)   // update App-level state
        setSaveMsg('ok')
        // Refresh stats after a short delay
        setTimeout(() => {
          fetch(BASE + '/api/threshold')
            .then(r => r.json())
            .then(setStats)
            .catch(() => {})
        }, 1000)
      } else {
        setSaveMsg('error')
      }
    } catch {
      setSaveMsg('error')
    } finally {
      setSaving(false)
      setTimeout(() => setSaveMsg(null), 4000)
    }
  }

  const isPending = Math.abs((draft ?? 0.5) - (activeThreshold ?? 0.5)) >= 0.01

  const pct = (n) => n == null ? '—' : (Number(n) * 100).toFixed(1) + '%'
  const fmt = (n) => n == null ? '—' : Number(n).toLocaleString()

  const safeDraft = draft ?? 0.5
  const thresholdColor = safeDraft < 0.3 ? 'text-red-400'
    : safeDraft < 0.5 ? 'text-amber-400'
    : safeDraft < 0.7 ? 'text-blue-400'
    : 'text-green-400'

  const thresholdLabel = safeDraft < 0.3 ? 'Very aggressive — high false positive risk'
    : safeDraft < 0.5 ? 'Aggressive — catches more fraud, some false positives'
    : safeDraft < 0.7 ? 'Balanced — default operating point'
    : 'Conservative — fewer false positives, may miss fraud'

  return (
    <div className="space-y-4">

      {/* Slider card */}
      <div className="card p-4">
        <div className="flex items-center justify-between mb-3">
          <p className="text-xs font-mono uppercase tracking-widest text-gray-500">
            Decision Threshold
          </p>
          <span className={`font-mono text-2xl font-bold ${thresholdColor}`}>
            {(draft ?? 0.5).toFixed(2)}
          </span>
        </div>

        <input
          type="range"
          min="0.10" max="0.90" step="0.01"
          value={draft ?? 0.5}
          onChange={e => setDraft(parseFloat(e.target.value))}
          className="w-full accent-blue-500"
        />

        <div className="flex justify-between text-[10px] font-mono text-gray-600 mt-1">
          <span>0.10 Aggressive</span>
          <span>0.50 Balanced</span>
          <span>0.90 Conservative</span>
        </div>

        <p className={`text-xs font-mono mt-2 ${thresholdColor}`}>
          {thresholdLabel}
        </p>

        {/* Status row */}
        <div className="mt-3 flex items-center gap-2 text-xs font-mono">
          <div className="w-2 h-2 rounded-full bg-green-500 pulse-dot flex-shrink-0" />
          <span className="text-gray-500">Active in consumer:</span>
          <span className="text-green-400 font-bold">{activeThreshold?.toFixed(2) ?? '…'}</span>
          {isPending && (
            <span className="text-amber-400 ml-1">
              → pending: {(draft ?? 0.5).toFixed(2)}
            </span>
          )}
        </div>

        {/* Apply button */}
        {isPending && (
          <button
            onClick={applyThreshold}
            disabled={saving}
            className="mt-3 w-full font-mono text-sm py-2 rounded-lg
              bg-blue-600 hover:bg-blue-500 disabled:opacity-50
              text-white transition-colors"
          >
            {saving ? 'Applying…' : `Apply ${(draft ?? 0.5).toFixed(2)}`}
          </button>
        )}

        {/* Save feedback */}
        {saveMsg === 'ok' && (
          <div className="mt-2 p-2 rounded-lg bg-green-950/40 border border-green-800/50">
            <p className="text-xs font-mono text-green-400 text-center">
              ✓ Threshold saved to Redis — consumer picks it up within 10 transactions
            </p>
          </div>
        )}
        {saveMsg === 'error' && (
          <div className="mt-2 p-2 rounded-lg bg-red-950/40 border border-red-800/50">
            <p className="text-xs font-mono text-red-400 text-center">
              ✗ Failed to save — check API is running
            </p>
          </div>
        )}
      </div>

      {/* Verify it worked — quick check button */}
      <div className="card p-3">
        <p className="text-[10px] font-mono uppercase tracking-widest text-gray-500 mb-2">
          Verify threshold is active
        </p>
        <p className="text-xs font-mono text-gray-400 mb-2">
          Check the consumer logs for:
        </p>
        <div className="bg-gray-950 rounded p-2 font-mono text-[10px] text-green-400 leading-relaxed">
          <div>Threshold updated: 0.50 → {activeThreshold?.toFixed(2) ?? '…'}</div>
        </div>
        <p className="text-[10px] font-mono text-gray-600 mt-2">
          Or check Redis directly:
        </p>
        <div className="bg-gray-950 rounded p-2 font-mono text-[10px] text-amber-400 mt-1">
          docker compose exec redis redis-cli GET fraud:threshold
        </div>
      </div>

      {/* Live impact metrics */}
      {stats && (
        <div>
          <p className="text-[10px] font-mono uppercase tracking-widest text-gray-500 mb-2">
            Impact at current threshold ({(activeThreshold ?? 0.5).toFixed(2)}) — all transactions
          </p>
          <div className="grid grid-cols-2 gap-2">
            <MetricPill label="Total Txns"      value={fmt(stats.total)}            color="text-gray-300" />
            <MetricPill label="Actual Fraud"    value={fmt(stats.actual_fraud)}     color="text-red-400" />
            <MetricPill label="Would Flag"      value={fmt(stats.would_flag)}       color="text-amber-400" />
            <MetricPill label="Flag Rate"       value={`${stats.flag_rate ?? '—'}%`} color="text-gray-300" />
            <MetricPill label="Precision"       value={pct(stats.precision)}        color="text-blue-400" />
            <MetricPill label="Recall"          value={pct(stats.recall)}           color="text-green-400" />
            <MetricPill label="False Positives" value={fmt(stats.false_positives)}  color="text-amber-400" />
            <MetricPill label="False Negatives" value={fmt(stats.false_negatives)}  color="text-red-400" />
            <MetricPill label="F1 Score"        value={stats.f1?.toFixed(3) ?? '—'} color="text-purple-400" />
            <MetricPill label="Avg Score"       value={stats.avg_score?.toFixed(3) ?? '—'} color="text-gray-400" />
          </div>
        </div>
      )}

      {/* Precision-Recall curve */}
      {curve.length > 0 && (
        <div className="card p-4">
          <p className="text-[10px] font-mono uppercase tracking-widest text-gray-500 mb-3">
            Precision–Recall tradeoff
          </p>
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={curve} margin={{ top: 5, right: 5, left: -20, bottom: 0 }}>
              <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
              <XAxis
                dataKey="threshold"
                tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'JetBrains Mono' }}
              />
              <YAxis tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'JetBrains Mono' }} domain={[0, 1]} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v, n) => [(v * 100).toFixed(1) + '%', n]} />
              <Legend wrapperStyle={{ fontSize: 11, fontFamily: 'JetBrains Mono' }} />
              <ReferenceLine
                x={activeThreshold}
                stroke="#3b82f6"
                strokeDasharray="4 4"
                label={{ value: 'active', fill: '#3b82f6', fontSize: 9 }}
              />
              {isPending && (
                <ReferenceLine
                  x={draft}
                  stroke="#f59e0b"
                  strokeDasharray="4 4"
                  label={{ value: 'pending', fill: '#f59e0b', fontSize: 9 }}
                />
              )}
              <Line type="monotone" dataKey="precision" stroke="#3b82f6" strokeWidth={2} dot={false} name="Precision" />
              <Line type="monotone" dataKey="recall"    stroke="#22c55e" strokeWidth={2} dot={false} name="Recall" />
              <Line type="monotone" dataKey="f1"        stroke="#a855f7" strokeWidth={1.5} dot={false} name="F1" strokeDasharray="4 2" />
            </LineChart>
          </ResponsiveContainer>
          <p className="text-[10px] font-mono text-gray-600 mt-1 text-center">
            Blue line = active threshold · Amber line = pending (not yet applied)
          </p>
        </div>
      )}

      {/* Explanation */}
      <div className="card p-3 border-blue-900/40 bg-blue-950/10">
        <p className="text-[11px] font-mono text-gray-400 leading-relaxed">
          <span className="text-blue-400 font-bold">How it works: </span>
          The model outputs a probability 0–1 per transaction. This threshold is the cutoff —
          scores above it are flagged as fraud. It is stored in Redis and the consumer reads it
          every 10 transactions without restarting. Lower = catch more fraud but block more
          legitimate transactions. Higher = fewer false positives but miss more fraud.
        </p>
      </div>
    </div>
  )
}