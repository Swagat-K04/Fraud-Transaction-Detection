import { useState, useCallback, useRef } from 'react'
import { useWebSocket } from './hooks/useWebSocket'
import { StatsBar }      from './components/StatsBar'
import { RiskChart }     from './components/RiskChart'
import { FeedPanel }     from './components/FeedPanel'
import { FraudAlerts }   from './components/FraudAlerts'
import { CustomerDrawer } from './components/CustomerDrawer'

const MAX_FEED    = 200   // cap in-memory feed length
const MAX_ALERTS  = 50

export default function App() {
  const [transactions, setTransactions] = useState([])
  const [alerts,       setAlerts]       = useState([])
  const [selectedTx,   setSelectedTx]   = useState(null)
  const [liveCount,    setLiveCount]    = useState(0)
  const [paused,       setPaused]       = useState(false)
  const pausedRef = useRef(false)

  const { connected } = useWebSocket(useCallback((msg) => {
    if (pausedRef.current) return

    const tx = {
      ...msg,
      // Preserve full cc_num for customer lookup, show masked in UI
      cc_num_full: msg.cc_num,
      cc_num: msg.cc_num?.slice(-4) || '????',
    }

    setTransactions(prev => [tx, ...prev].slice(0, MAX_FEED))
    setLiveCount(c => c + 1)

    if (tx.is_fraud) {
      setAlerts(prev => [tx, ...prev].slice(0, MAX_ALERTS))
    }
  }, []))

  const togglePause = () => {
    pausedRef.current = !pausedRef.current
    setPaused(pausedRef.current)
  }

  const clearAll = () => {
    setTransactions([])
    setAlerts([])
    setLiveCount(0)
  }

  return (
    <div className="h-screen flex flex-col overflow-hidden bg-gray-950">

      {/* ── Header ─────────────────────────────────────────────────── */}
      <header className="flex-shrink-0 flex items-center justify-between
        px-5 py-3 border-b border-gray-800 bg-gray-950 z-10">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-red-500 rounded-lg flex items-center
            justify-center font-bold text-sm text-white font-display">F</div>
          <span className="font-display font-bold text-lg tracking-tight">
            Fraud<span className="text-red-400">Radar</span>
          </span>
          <div className={`flex items-center gap-1.5 font-mono text-[11px] px-2 py-1
            rounded-full border ${connected
              ? 'text-green-400 bg-green-950/50 border-green-800/50'
              : 'text-gray-500 bg-gray-900 border-gray-700'
            }`}>
            <div className={`w-1.5 h-1.5 rounded-full ${connected ? 'bg-green-400 pulse-dot' : 'bg-gray-600'}`} />
            {connected ? 'LIVE' : 'CONNECTING'}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <span className="font-mono text-xs text-gray-600">
            {transactions.length} tx · {alerts.length} alerts
          </span>
          <button
            onClick={togglePause}
            className="font-mono text-xs px-3 py-1.5 rounded-lg border
              border-gray-700 hover:border-gray-600 text-gray-300 hover:text-white transition-colors"
          >
            {paused ? '▶ Resume' : '⏸ Pause'}
          </button>
          <button
            onClick={clearAll}
            className="font-mono text-xs px-3 py-1.5 rounded-lg border
              border-gray-700 hover:border-red-800 text-gray-500 hover:text-red-400 transition-colors"
          >
            Clear
          </button>
        </div>
      </header>

      {/* ── Stats bar ──────────────────────────────────────────────── */}
      <div className="flex-shrink-0 px-4 pt-4 pb-3">
        <StatsBar liveCount={liveCount} />
      </div>

      {/* ── Charts ─────────────────────────────────────────────────── */}
      <div className="flex-shrink-0 px-4 pb-3">
        <RiskChart liveCount={liveCount} />
      </div>

      {/* ── Main content — 3-column grid ───────────────────────────── */}
      <div className="flex-1 min-h-0 grid grid-cols-12 gap-3 px-4 pb-4">

        {/* Live feed — 7 cols */}
        <div className="col-span-7 card p-3 overflow-hidden flex flex-col">
          <FeedPanel
            transactions={transactions}
            onSelectTx={setSelectedTx}
          />
        </div>

        {/* Fraud alerts — 5 cols */}
        <div className="col-span-5 card p-3 overflow-hidden flex flex-col">
          <FraudAlerts alerts={alerts} />
        </div>
      </div>

      {/* ── Customer detail drawer ─────────────────────────────────── */}
      {selectedTx && (
        <CustomerDrawer
          tx={selectedTx}
          onClose={() => setSelectedTx(null)}
        />
      )}
    </div>
  )
}
