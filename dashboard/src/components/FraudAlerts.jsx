import { format } from 'date-fns'

function ShapBar({ feature }) {
  const isPositive = feature.shap > 0
  const width = Math.min(Math.abs(feature.shap) * 200, 100)
  return (
    <div className="flex items-center gap-2 text-xs font-mono">
      <span className="w-28 truncate text-gray-500 text-[10px]">
        {feature.feature.replace('_', ' ')}
      </span>
      <div className="flex-1 h-1 bg-gray-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full ${isPositive ? 'bg-red-500' : 'bg-green-600'}`}
          style={{ width: `${width}%` }}
        />
      </div>
      <span className={`w-12 text-right text-[10px] ${isPositive ? 'text-red-400' : 'text-green-400'}`}>
        {feature.shap > 0 ? '+' : ''}{feature.shap.toFixed(3)}
      </span>
    </div>
  )
}

function AlertCard({ tx }) {
  const time = tx.trans_time
    ? format(new Date(tx.trans_time), 'HH:mm:ss')
    : '—'
  const pct = Math.round((tx.fraud_score || 0) * 100)

  const riskColor = {
    CRITICAL: 'border-red-700/60 bg-red-950/30',
    HIGH:     'border-amber-700/60 bg-amber-950/20',
    MEDIUM:   'border-blue-700/60 bg-blue-950/20',
    LOW:      'border-green-800/60 bg-green-950/20',
  }[tx.risk_level] || 'border-gray-700 bg-gray-900/50'

  return (
    <div className={`slide-in p-3 rounded-lg border mb-2 ${riskColor}`}>
      {/* Header */}
      <div className="flex items-start justify-between gap-2 mb-1">
        <div className="min-w-0">
          <p className="font-bold text-sm truncate">{tx.merchant}</p>
          <p className="text-[10px] font-mono text-gray-500">
            ••{tx.cc_num} · {tx.category?.replace('_', ' ')} · {time}
          </p>
        </div>
        <div className="text-right flex-shrink-0">
          <p className="font-mono font-bold text-red-400">${Number(tx.amt).toFixed(2)}</p>
          <span className={`text-[10px] font-mono font-bold px-1.5 py-0.5 rounded uppercase ${
            tx.risk_level === 'CRITICAL' ? 'bg-red-800/60 text-red-300' :
            tx.risk_level === 'HIGH'     ? 'bg-amber-800/60 text-amber-300' :
            'bg-gray-800 text-gray-400'
          }`}>{tx.risk_level}</span>
        </div>
      </div>

      {/* AI Reasoning */}
      {tx.reasoning && (
        <p className="text-xs text-gray-300 mb-2 leading-relaxed border-l-2 border-red-700/50 pl-2">
          {tx.reasoning}
        </p>
      )}

      {/* Risk score bar */}
      <div className="flex items-center gap-2 mb-2">
        <span className="text-[10px] font-mono text-gray-600">Risk</span>
        <div className="flex-1 h-1.5 bg-gray-800 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full bg-red-500 transition-all duration-700"
            style={{ width: `${pct}%` }}
          />
        </div>
        <span className="text-[10px] font-mono text-red-400 w-8 text-right">{pct}%</span>
      </div>

      {/* SHAP feature attributions */}
      {tx.top_features?.length > 0 && (
        <div className="space-y-1 pt-1 border-t border-gray-800/60">
          <p className="text-[10px] font-mono text-gray-600 uppercase tracking-wider mb-1">
            SHAP attribution
          </p>
          {tx.top_features.map((f, i) => (
            <ShapBar key={i} feature={f} />
          ))}
        </div>
      )}
    </div>
  )
}

export function FraudAlerts({ alerts }) {
  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-mono uppercase tracking-widest text-gray-500">
          🚨 Fraud Alerts
        </p>
        {alerts.length > 0 && (
          <span className="font-mono text-xs bg-red-900/50 text-red-300 border border-red-800/50 px-2 py-0.5 rounded-full">
            {alerts.length}
          </span>
        )}
      </div>

      <div className="flex-1 overflow-y-auto pr-1">
        {alerts.length === 0 ? (
          <div className="text-center py-16 text-gray-600">
            <div className="text-3xl mb-2">🛡️</div>
            <p className="text-sm font-mono">No fraud detected yet</p>
          </div>
        ) : (
          alerts.slice(0, 30).map(tx => (
            <AlertCard key={tx.trans_num} tx={tx} />
          ))
        )}
      </div>
    </div>
  )
}
