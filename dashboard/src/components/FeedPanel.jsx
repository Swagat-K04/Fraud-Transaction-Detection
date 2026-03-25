import { useState, useCallback } from 'react'
import { format, parseISO } from 'date-fns'

const RISK_CLASS = {
  CRITICAL: 'risk-critical',
  HIGH:     'risk-high',
  MEDIUM:   'risk-medium',
  LOW:      'risk-low',
}

const CATEGORY_ICONS = {
  grocery_pos:    '🛒', gas_transport: '⛽', home: '🏠',
  shopping_net:   '💻', shopping_pos: '🏪', food_dining: '🍔',
  health_fitness: '💊', entertainment: '🎬', travel: '✈️',
  personal_care:  '💄', kids_pets: '🐾', misc_net: '❓', misc_pos: '🔲',
}

function RiskBadge({ level }) {
  if (!level) return null
  return (
    <span className={`inline-block font-mono text-[10px] font-bold px-2 py-0.5 rounded-full uppercase tracking-wide ${RISK_CLASS[level] || 'risk-low'}`}>
      {level}
    </span>
  )
}

function ScoreBar({ score }) {
  const pct = Math.round((score || 0) * 100)
  const color = pct >= 75 ? '#ef4444' : pct >= 55 ? '#f59e0b' : pct >= 30 ? '#3b82f6' : '#22c55e'
  return (
    <div className="flex items-center gap-2 mt-1">
      <div className="flex-1 h-1 bg-gray-800 rounded-full overflow-hidden">
        <div className="h-full rounded-full transition-all duration-500" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="font-mono text-[10px] text-gray-500 w-8 text-right">{pct}%</span>
    </div>
  )
}

function TxCard({ tx, onSelect }) {
  const isFraud = tx.is_fraud
  const time = tx.trans_time
    ? format(new Date(tx.trans_time), 'HH:mm:ss')
    : '—'
  const icon = CATEGORY_ICONS[tx.category] || '💳'

  return (
    <div
      onClick={() => onSelect(tx)}
      className={`slide-in flex gap-3 items-start p-3 rounded-lg border cursor-pointer transition-all duration-150 hover:border-gray-700
        ${isFraud
          ? 'border-red-900/60 bg-red-950/20 hover:bg-red-950/30'
          : 'border-gray-800 bg-gray-900/60 hover:bg-gray-900'
        }`}
    >
      {/* Icon */}
      <div className={`flex-shrink-0 w-9 h-9 rounded-lg flex items-center justify-center text-lg
        ${isFraud ? 'bg-red-900/50' : 'bg-gray-800'}`}>
        {icon}
      </div>

      {/* Body */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-0.5">
          <span className="font-bold text-sm truncate">{tx.merchant}</span>
          <RiskBadge level={tx.risk_level} />
        </div>
        <div className="flex items-center gap-2 text-xs text-gray-500 font-mono flex-wrap">
          <span>{tx.category?.replace('_', ' ')}</span>
          <span>·</span>
          <span>••{tx.cc_num}</span>
          <span>·</span>
          <span>{time}</span>
        </div>
        {isFraud && tx.reasoning && (
          <p className="text-xs text-red-400 mt-1 leading-relaxed line-clamp-2">
            ⚠ {tx.reasoning}
          </p>
        )}
        <ScoreBar score={tx.fraud_score} />
      </div>

      {/* Amount */}
      <div className="flex-shrink-0 text-right">
        <p className={`font-mono font-bold text-base ${isFraud ? 'text-red-400' : 'text-gray-100'}`}>
          ${Number(tx.amt).toFixed(2)}
        </p>
      </div>
    </div>
  )
}

const FILTERS = [
  { key: 'all',    label: 'All' },
  { key: 'fraud',  label: 'Fraud' },
  { key: 'safe',   label: 'Safe' },
]

export function FeedPanel({ transactions, onSelectTx }) {
  const [filter, setFilter] = useState('all')

  const visible = transactions.filter(tx => {
    if (filter === 'fraud') return tx.is_fraud === true
    if (filter === 'safe')  return tx.is_fraud === false
    return true
  }).slice(0, 60)

  return (
    <div className="flex flex-col h-full">
      {/* Filter tabs */}
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-mono uppercase tracking-widest text-gray-500">Live Feed</p>
        <div className="flex gap-1">
          {FILTERS.map(f => (
            <button
              key={f.key}
              onClick={() => setFilter(f.key)}
              className={`font-mono text-xs px-3 py-1 rounded-full border transition-all
                ${filter === f.key
                  ? 'bg-gray-100 text-gray-900 border-gray-100'
                  : 'border-gray-700 text-gray-500 hover:border-gray-600'
                }`}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>

      {/* Feed */}
      <div className="flex-1 overflow-y-auto space-y-2 pr-1">
        {visible.length === 0 ? (
          <div className="text-center py-16 text-gray-600">
            <div className="text-3xl mb-2">📡</div>
            <p className="text-sm font-mono">Awaiting transactions...</p>
          </div>
        ) : (
          visible.map(tx => (
            <TxCard key={`${tx.trans_num}-${tx.trans_time}`} tx={tx} onSelect={onSelectTx} />
          ))
        )}
      </div>
    </div>
  )
}