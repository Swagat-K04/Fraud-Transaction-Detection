import { useEffect, useRef, useCallback, useState } from 'react'

const WS_URL = (import.meta.env.VITE_API_URL || 'http://localhost:8000')
  .replace('http', 'ws') + '/ws'

export function useWebSocket(onMessage) {
  const ws = useRef(null)
  const [connected, setConnected] = useState(false)
  const reconnectTimer = useRef(null)

  const connect = useCallback(() => {
    if (ws.current?.readyState === WebSocket.OPEN) return

    ws.current = new WebSocket(WS_URL)

    ws.current.onopen = () => {
      setConnected(true)
      console.log('[WS] Connected')
    }

    ws.current.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.type === 'ping') return
        onMessage(data)
      } catch {}
    }

    ws.current.onclose = () => {
      setConnected(false)
      console.log('[WS] Disconnected — reconnecting in 2s')
      reconnectTimer.current = setTimeout(connect, 2000)
    }

    ws.current.onerror = () => {
      ws.current?.close()
    }
  }, [onMessage])

  useEffect(() => {
    connect()
    return () => {
      clearTimeout(reconnectTimer.current)
      ws.current?.close()
    }
  }, [connect])

  return { connected }
}
