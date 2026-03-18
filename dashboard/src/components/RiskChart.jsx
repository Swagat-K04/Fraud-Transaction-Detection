import { useEffect, useState } from 'react'
import {
  AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend
} from 'recharts'
import { format, parseISO } from 'date-fns'
import { api } from '../lib/api'

const TOOLTIP_STYLE = {
  backgroundColor: '#111827',
  border: '1px solid #374151',
  borderRadius: 8,
  fontFamily: "'JetBrains Mono', monospace",
  fontSize: 12,
  color: '#f3f4f6',
}

export function RiskChart({ liveCount }) {
  const [hourly, setHourly]       = useState([])
  const [byCategory, setCategory] = useState([])

  useEffect(() => {
    const load = () => {
      api.hourly().then(rows => setHourly(rows.map(r => ({
        ...r,
        hour: format(parseISO(r.hour), 'HH:mm'),
        fraud_rate: r.total > 0 ? +((r.fraud_count / r.total) * 100).toFixed(1) : 0,
      })))).catch(() => {})
      api.byCategory().then(rows => setCategory(
        rows.slice(0, 8).map(r => ({ ...r, name: r.category.replace('_', ' ') }))
      )).catch(() => {})
    }
    load()
    const t = setInterval(load, 10000)
    return () => clearInterval(t)
  }, [liveCount])

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
      {/* Hourly area chart */}
      <div className="card p-4">
        <p className="text-xs font-mono uppercase tracking-widest text-gray-500 mb-4">
          Transaction volume — last 24h
        </p>
        <ResponsiveContainer width="100%" height={180}>
          <AreaChart data={hourly} margin={{ top: 5, right: 5, left: -20, bottom: 0 }}>
            <defs>
              <linearGradient id="totalGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#3b82f6" stopOpacity={0.3}/>
                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
              </linearGradient>
              <linearGradient id="fraudGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#ef4444" stopOpacity={0.5}/>
                <stop offset="95%" stopColor="#ef4444" stopOpacity={0}/>
              </linearGradient>
            </defs>
            <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
            <XAxis dataKey="hour" tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'JetBrains Mono' }} />
            <YAxis tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'JetBrains Mono' }} />
            <Tooltip contentStyle={TOOLTIP_STYLE} />
            <Area type="monotone" dataKey="total"       stroke="#3b82f6" fill="url(#totalGrad)" strokeWidth={1.5} name="Total" />
            <Area type="monotone" dataKey="fraud_count" stroke="#ef4444" fill="url(#fraudGrad)" strokeWidth={1.5} name="Fraud" />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Category bar chart */}
      <div className="card p-4">
        <p className="text-xs font-mono uppercase tracking-widest text-gray-500 mb-4">
          Fraud by merchant category
        </p>
        <ResponsiveContainer width="100%" height={180}>
          <BarChart data={byCategory} layout="vertical" margin={{ top: 0, right: 5, left: 10, bottom: 0 }}>
            <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" horizontal={false} />
            <XAxis type="number" tick={{ fill: '#6b7280', fontSize: 10, fontFamily: 'JetBrains Mono' }} />
            <YAxis dataKey="name" type="category" width={90}
              tick={{ fill: '#9ca3af', fontSize: 10, fontFamily: 'JetBrains Mono' }} />
            <Tooltip contentStyle={TOOLTIP_STYLE} />
            <Bar dataKey="fraud_count" fill="#ef4444" radius={[0, 3, 3, 0]} name="Fraud" opacity={0.85} />
            <Bar dataKey="total"       fill="#3b82f6" radius={[0, 3, 3, 0]} name="Total" opacity={0.35} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
