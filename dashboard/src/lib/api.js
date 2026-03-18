const BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000'

async function get(path) {
  const r = await fetch(BASE + path)
  if (!r.ok) throw new Error(`API ${path} → ${r.status}`)
  return r.json()
}

export const api = {
  summary:         () => get('/api/stats/summary'),
  hourly:          () => get('/api/stats/hourly'),
  byCategory:      () => get('/api/stats/by-category'),
  riskDist:        () => get('/api/stats/risk-distribution'),
  transactions:    (params = '') => get(`/api/transactions?${params}`),
  transaction:     (id) => get(`/api/transactions/${id}`),
  customer:        (cc) => get(`/api/customer/${cc}`),
  statement:       (cc) => get(`/api/statement/${cc}`),
}
