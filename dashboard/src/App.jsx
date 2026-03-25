import { useState, useCallback, useRef, useEffect } from 'react'
import { useWebSocket }     from './hooks/useWebSocket'
import { StatsBar }         from './components/StatsBar'
import { RiskChart }        from './components/RiskChart'
import { FeedPanel }        from './components/FeedPanel'
import { FraudAlerts }      from './components/FraudAlerts'
import { CustomerDrawer }   from './components/CustomerDrawer'
import { ThresholdPanel }   from './components/ThresholdPanel'

const MAX_FEED   = 200
const MAX_ALERTS = 50

const SIDE_TABS = [
  { key: 'alerts',    label: '🚨 Alerts' },
  { key: 'threshold', label: '⚙️ Threshold' },
]

export default function App() {
  const [transactions, setTransactions] = useState([])
  const [alerts,       setAlerts]       = useState([])
  const [selectedTx,   setSelectedTx]   = useState(null)
  const [liveCount,    setLiveCount]    = useState(0)
  const [paused,       setPaused]       = useState(false)
  const [sideTab,      setSideTab]      = useState('alerts')
  const [activeThreshold, setActiveThreshold] = useState(0.5)
  const [injecting, setInjecting] = useState(false)
  const [injectResult, setInjectResult] = useState(null)
  const [injectScenario, setInjectScenario] = useState(0)

  // Fetch active threshold from API on mount
  useEffect(() => {
    fetch((import.meta.env.VITE_API_URL || 'http://localhost:8000') + '/api/threshold')
      .then(r => r.json())
      .then(d => { if (d?.threshold != null) setActiveThreshold(d.threshold) })
      .catch(() => {})
  }, [])
  const pausedRef = useRef(false)

  const { connected } = useWebSocket(useCallback((msg) => {
    // Handle threshold change broadcast from Redis
    if (msg.threshold !== undefined && msg.trans_num === undefined) {
      setActiveThreshold(msg.threshold)
      return
    }

    if (pausedRef.current) return

    const tx = {
      ...msg,
      cc_num_full: msg.cc_num,
      cc_num: msg.cc_num?.slice(-4) || '????',
    }

    setTransactions(prev => [tx, ...prev].slice(0, MAX_FEED))
    setLiveCount(c => c + 1)

    if (tx.is_fraud) {
      setAlerts(prev => [tx, ...prev].slice(0, MAX_ALERTS))
      // Auto-switch to alerts tab when new fraud detected
      setSideTab('alerts')
    }
  }, []))

  const injectFraud = async () => {
    setInjecting(true)
    setInjectResult(null)
    try {
      const res = await fetch(
        (import.meta.env.VITE_API_URL || 'http://localhost:8000') + '/api/inject/fraud',
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ scenario: injectScenario }) }
      )
      const data = await res.json()
      setInjectResult(data)
      setSideTab('alerts')
      setTimeout(() => setInjectResult(null), 5000)
    } catch (e) {
      setInjectResult({ success: false, message: 'Failed: ' + e.message })
    } finally {
      setInjecting(false)
    }
  }

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
          {/* Active threshold badge */}
          <div className="flex items-center gap-1.5 font-mono text-[11px] px-2 py-1
            rounded-full border border-blue-800/50 text-blue-400 bg-blue-950/30">
            ⚙ threshold: {activeThreshold?.toFixed(2) ?? '…'}
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
          <select
            value={injectScenario}
            onChange={e => setInjectScenario(Number(e.target.value))}
            className="font-mono text-xs px-2 py-1.5 rounded-lg border
              border-gray-700 bg-gray-900 text-gray-400"
          >
            <option value={0}>Card-not-present</option>
            <option value={1}>Velocity fraud</option>
            <option value={2}>Geographic anomaly</option>
          </select>
          <button
            onClick={injectFraud}
            disabled={injecting}
            className="font-mono text-xs px-3 py-1.5 rounded-lg border
              border-red-800/60 hover:border-red-500 text-red-400 hover:text-red-300
              disabled:opacity-50 transition-colors"
          >
            {injecting ? '⏳ Injecting…' : '🧪 Inject Fraud'}
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

      {/* ── Main 3-column grid ─────────────────────────────────────── */}
      <div className="flex-1 min-h-0 grid grid-cols-12 gap-3 px-4 pb-4">

        {/* Live feed — 7 cols */}
        <div className="col-span-7 card p-3 overflow-hidden flex flex-col">
          <FeedPanel
            transactions={transactions}
            onSelectTx={setSelectedTx}
          />
        </div>

        {/* Right panel — 5 cols with tabs */}
        <div className="col-span-5 card overflow-hidden flex flex-col">
          {/* Tab bar */}
          <div className="flex border-b border-gray-800 flex-shrink-0">
            {SIDE_TABS.map(tab => (
              <button
                key={tab.key}
                onClick={() => setSideTab(tab.key)}
                className={`flex-1 py-2.5 font-mono text-xs transition-colors
                  ${sideTab === tab.key
                    ? 'text-gray-100 border-b-2 border-blue-500 bg-gray-900/50'
                    : 'text-gray-500 hover:text-gray-300'
                  }`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {/* Tab content */}
          <div className="flex-1 overflow-y-auto p-3">
            {sideTab === 'alerts' && <FraudAlerts alerts={alerts} />}
            {sideTab === 'threshold' && (
              <ThresholdPanel onThresholdChange={setActiveThreshold} />
            )}
          </div>
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