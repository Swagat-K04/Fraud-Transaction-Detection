import { useEffect, useState } from 'react'
import { api } from '../lib/api'

function StatCard({ label, value, sub, accent, barPct }) {
  return (
    <div className={`card p-4 ${accent === 'red' ? 'border-red-800/60' : ''}`}>
      <p className="text-xs font-mono uppercase tracking-widest text-gray-500 mb-1">{label}</p>
      <p className={`text-3xl font-mono font-bold leading-none ${
        accent === 'red' ? 'text-red-400' : accent === 'amber' ? 'text-amber-400' : 'text-gray-100'
      }`}>{value}</p>
      {sub && <p className="text-xs font-mono text-gray-500 mt-1">{sub}</p>}
      {barPct !== undefined && (
        <div className="mt-3 h-0.5 bg-gray-800 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all duration-700 ${
              accent === 'red' ? 'bg-red-500' : 'bg-amber-500'
            }`}
            style={{ width: `${Math.min(barPct, 100)}%` }}
          />
        </div>
      )}
    </div>
  )
}

export function StatsBar({ liveCount }) {
  const [stats, setStats] = useState(null)

  useEffect(() => {
    const load = () => api.summary().then(setStats).catch(() => {})
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [liveCount])

  const fmt = (n) => n == null ? '—' : Number(n).toLocaleString()
  const fmtAmt = (n) => n == null ? '—' : '$' + Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 })

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
      <StatCard
        label="Transactions (24h)"
        value={fmt(stats?.total)}
        sub="total processed"
      />
      <StatCard
        label="Fraud Detected"
        value={fmt(stats?.fraud_count)}
        sub={`${stats?.fraud_rate ?? '0.0'}% fraud rate`}
        accent="red"
        barPct={stats?.fraud_rate}
      />
      <StatCard
        label="Total Volume"
        value={fmtAmt(stats?.total_volume)}
        sub="24h processed"
      />
      <StatCard
        label="Fraud Intercepted"
        value={fmtAmt(stats?.fraud_volume)}
        sub={`${fmt(stats?.critical_count)} critical alerts`}
        accent="amber"
      />
    </div>
  )
}
