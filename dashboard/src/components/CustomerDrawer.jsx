import { useEffect, useState } from 'react'
import { format } from 'date-fns'
import { api } from '../lib/api'

const RISK_COLOR = {
  CRITICAL: 'text-red-400',
  HIGH:     'text-amber-400',
  MEDIUM:   'text-blue-400',
  LOW:      'text-green-400',
}

function Row({ label, value, mono, accent }) {
  return (
    <div className="flex items-start justify-between gap-4 py-2 border-b border-gray-800/60 last:border-0">
      <span className="text-xs text-gray-500 shrink-0 w-32">{label}</span>
      <span className={`text-xs text-right break-all ${mono ? 'font-mono' : ''} ${accent || 'text-gray-200'}`}>
        {value ?? '—'}
      </span>
    </div>
  )
}

function FeatureRow({ f }) {
  const pct = Math.min(Math.abs(f.shap) * 200, 100)
  return (
    <div className="flex items-center gap-2 py-1">
      <span className="text-[10px] font-mono text-gray-500 w-28 truncate">{f.feature.replace(/_/g, ' ')}</span>
      <div className="flex-1 h-1 bg-gray-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full ${f.shap > 0 ? 'bg-red-500' : 'bg-green-600'}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className={`text-[10px] font-mono w-14 text-right ${f.shap > 0 ? 'text-red-400' : 'text-green-400'}`}>
        {f.shap > 0 ? '+' : ''}{f.shap.toFixed(3)}
      </span>
    </div>
  )
}

export function CustomerDrawer({ tx, onClose }) {
  const [customer, setCustomer] = useState(null)
  const [history, setHistory]   = useState([])
  const [loading, setLoading]   = useState(false)

  useEffect(() => {
    if (!tx) return
    setLoading(true)
    setCustomer(null)
    setHistory([])
    api.customer(tx.cc_num_full || tx.cc_num)
      .then(data => {
        setCustomer(data.customer)
        setHistory(data.transactions || [])
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [tx])

  if (!tx) return null

  const score = Math.round((tx.fraud_score || 0) * 100)

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/60 z-40"
        onClick={onClose}
      />

      {/* Drawer */}
      <div className="fixed right-0 top-0 h-full w-[420px] bg-gray-950 border-l border-gray-800 z-50 flex flex-col overflow-hidden shadow-2xl">

        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-800">
          <div>
            <p className="font-bold text-base">{tx.merchant}</p>
            <p className="font-mono text-xs text-gray-500 mt-0.5">
              {tx.trans_num?.slice(0, 16)}…
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-200 text-xl leading-none px-2"
          >×</button>
        </div>

        <div className="flex-1 overflow-y-auto">

          {/* Verdict banner */}
          <div className={`mx-4 mt-4 p-3 rounded-lg border ${
            tx.is_fraud
              ? 'bg-red-950/40 border-red-800/60'
              : 'bg-green-950/30 border-green-800/50'
          }`}>
            <div className="flex items-center justify-between">
              <span className={`font-bold text-sm ${tx.is_fraud ? 'text-red-400' : 'text-green-400'}`}>
                {tx.is_fraud ? '⚠ FRAUD DETECTED' : '✓ LEGITIMATE'}
              </span>
              <span className={`font-mono text-xs font-bold ${RISK_COLOR[tx.risk_level] || 'text-gray-400'}`}>
                {tx.risk_level}
              </span>
            </div>
            <div className="mt-2 flex items-center gap-2">
              <div className="flex-1 h-1.5 bg-gray-800 rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full ${tx.is_fraud ? 'bg-red-500' : 'bg-green-600'}`}
                  style={{ width: `${score}%` }}
                />
              </div>
              <span className="font-mono text-xs text-gray-400">{score}% risk</span>
            </div>
            {tx.reasoning && (
              <p className="text-xs text-gray-300 mt-2 leading-relaxed">{tx.reasoning}</p>
            )}
          </div>

          {/* Transaction details */}
          <div className="px-4 mt-4">
            <p className="text-[10px] font-mono uppercase tracking-widest text-gray-600 mb-2">Transaction</p>
            <Row label="Amount"    value={`$${Number(tx.amt).toFixed(2)}`} mono accent={tx.is_fraud ? 'text-red-400' : 'text-gray-100'} />
            <Row label="Category"  value={tx.category?.replace(/_/g, ' ')} />
            <Row label="Card"      value={`••${tx.cc_num}`} mono />
            <Row label="Time"      value={tx.trans_time ? format(new Date(tx.trans_time), 'PPpp') : '—'} mono />
            <Row label="Partition" value={tx.kafka_partition} mono />
            <Row label="Offset"    value={tx.kafka_offset}    mono />
          </div>

          {/* SHAP feature attributions */}
          {tx.top_features?.length > 0 && (
            <div className="px-4 mt-4">
              <p className="text-[10px] font-mono uppercase tracking-widest text-gray-600 mb-2">
                Model attribution (SHAP)
              </p>
              <div className="card p-3">
                {tx.top_features.map((f, i) => <FeatureRow key={i} f={f} />)}
              </div>
            </div>
          )}

          {/* Customer info */}
          <div className="px-4 mt-4">
            <p className="text-[10px] font-mono uppercase tracking-widest text-gray-600 mb-2">Customer</p>
            {loading ? (
              <div className="h-20 analyzing-shimmer rounded-lg" />
            ) : customer ? (
              <div className="card p-3">
                <Row label="Name"  value={`${customer.first_name} ${customer.last_name}`} />
                <Row label="Job"   value={customer.job} />
                <Row label="City"  value={`${customer.city}, ${customer.state}`} />
                <Row label="DOB"   value={customer.dob} mono />
              </div>
            ) : (
              <p className="text-xs text-gray-600 font-mono">Customer data unavailable</p>
            )}
          </div>

          {/* Transaction history */}
          {history.length > 0 && (
            <div className="px-4 mt-4 mb-6">
              <p className="text-[10px] font-mono uppercase tracking-widest text-gray-600 mb-2">
                Recent history ({history.length})
              </p>
              <div className="space-y-1.5">
                {history.slice(0, 8).map(h => (
                  <div key={h.trans_num}
                    className={`flex items-center gap-2 p-2 rounded-lg text-xs border ${
                      h.is_fraud ? 'border-red-900/50 bg-red-950/20' : 'border-gray-800 bg-gray-900/50'
                    }`}
                  >
                    <span className={`font-mono ${h.is_fraud ? 'text-red-400' : 'text-gray-400'}`}>
                      ${Number(h.amt).toFixed(2)}
                    </span>
                    <span className="text-gray-500 truncate flex-1">{h.merchant}</span>
                    <span className={`text-[10px] font-mono ${RISK_COLOR[h.risk_level] || 'text-gray-600'}`}>
                      {h.risk_level}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  )
}
