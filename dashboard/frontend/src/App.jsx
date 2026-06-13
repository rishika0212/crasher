import React, { useState, useEffect, useRef } from 'react'
import { Activity, ShieldAlert, Terminal, MessageSquareCode } from 'lucide-react'
import ServiceCard from './components/ServiceCard.jsx'
import LiveChart from './components/LiveChart.jsx'
import ChaosPanel from './components/ChaosPanel.jsx'

export default function App() {
  const [connected, setConnected] = useState(false)
  const [services, setServices] = useState({})
  const [kafkaLag, setKafkaLag] = useState(0)
  const [activeChaos, setActiveChaos] = useState({})
  const [logs, setLogs] = useState([])
  const [history, setHistory] = useState([])

  const wsRef = useRef(null)
  const logStreamRef = useRef(null)

  // Auto-scroll log console
  useEffect(() => {
    if (logStreamRef.current) {
      logStreamRef.current.scrollTop = logStreamRef.current.scrollHeight
    }
  }, [logs])

  // WebSocket Connection
  useEffect(() => {
    let reconnectTimeout

    const connectWS = () => {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      const wsUrl = `${protocol}//${window.location.host}/ws`
      
      console.log(`Connecting to WebSocket at: ${wsUrl}`)
      const ws = new WebSocket(wsUrl)
      wsRef.current = ws

      ws.onopen = () => {
        console.log('WebSocket connection established')
        setConnected(true)
      }

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          const { services: svcData, kafka_lag, chaos, logs: lokiLogs } = data

          setServices(svcData || {})
          setKafkaLag(kafka_lag || 0)
          setActiveChaos(chaos?.active || {})
          
          if (lokiLogs) {
            setLogs(lokiLogs)
          }

          // Buffer history up to last 30 data points
          setHistory(prev => {
            const next = [...prev, data]
            if (next.length > 30) {
              next.shift()
            }
            return next
          })
        } catch (err) {
          console.error('Failed to parse WebSocket message:', err)
        }
      }

      ws.onclose = () => {
        console.log('WebSocket connection closed. Reconnecting...')
        setConnected(false)
        reconnectTimeout = setTimeout(connectWS, 2000)
      }

      ws.onerror = (err) => {
        console.error('WebSocket connection error:', err)
        ws.close()
      }
    }

    connectWS()

    return () => {
      if (wsRef.current) wsRef.current.close()
      clearTimeout(reconnectTimeout)
    }
  }, [])

  // Render lists of active cards
  const serviceList = Object.values(services)

  return (
    <div className="dashboard-container">
      {/* Header */}
      <header className="glass">
        <h1>
          💥 CrashBoard
          <span style={{ fontSize: '12px', fontWeight: '500', color: 'var(--text-muted)', borderLeft: '1px solid rgba(255,255,255,0.1)', paddingLeft: '12px', marginLeft: '2px' }}>
            Chaos Orchestration & Observability Control Room
          </span>
        </h1>
        
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <span className="status-badge" style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '11px', background: connected ? 'rgba(34,211,238,0.06)' : 'rgba(248,113,113,0.06)', color: connected ? 'var(--color-cyan)' : 'var(--color-red)' }}>
            <span 
              className="pulsing-dot" 
              style={{ 
                backgroundColor: connected ? 'var(--color-cyan)' : 'var(--color-red)',
                boxShadow: connected ? '0 0 10px var(--color-cyan)' : '0 0 10px var(--color-red)',
                animation: connected ? 'pulse 1.8s infinite' : 'none'
              }} 
            />
            {connected ? 'BROADCAST ONLINE' : 'BROADCAST DISCONNECTED'}
          </span>
        </div>
      </header>

      {/* Main Grid */}
      <main className="dashboard-grid">
        {/* Left column: Microservices & Graphs */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
          {/* Services list */}
          <div className="services-list">
            {serviceList.length > 0 ? (
              serviceList.map(svc => (
                <ServiceCard key={svc.name} service={svc} />
              ))
            ) : (
              // Loading cards placeholder
              Array.from({ length: 4 }).map((_, idx) => (
                <div key={idx} className="glass service-card" style={{ height: '180px', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                  <span className="text-muted" style={{ fontStyle: 'italic', fontSize: '14px' }}>Connecting to data streams...</span>
                </div>
              ))
            )}
          </div>

          {/* Real-time telemetry charts */}
          <LiveChart history={history} />
        </div>

        {/* Right column: Chaos Controls & Queue Status */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
          {/* Kafka Lag ring */}
          <div className="glass kafka-lag-box">
            <h3 style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '15px', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
              <ShieldAlert size={16} className={kafkaLag > 100 ? 'metric-value yellow animate-pulse' : 'text-muted'} />
              <span>Kafka Consumer Backlog</span>
            </h3>
            
            <div className={`lag-ring ${kafkaLag > 500 ? 'spiking' : ''}`}>
              <div className={`lag-value ${kafkaLag > 0 ? 'active' : ''}`}>
                {kafkaLag}
              </div>
              <div className="lag-label">messages</div>
            </div>
          </div>

          {/* Chaos Panel */}
          <ChaosPanel activeChaos={activeChaos} />
        </div>
      </main>

      {/* Footer log streams */}
      <div className="footer-row">
        <div className="glass logs-card">
          <h3>
            <Terminal size={18} className="metric-value purple" />
            <span>Loki Distributed Log Streams</span>
          </h3>
          
          <div className="log-stream" ref={logStreamRef}>
            {logs.length > 0 ? (
              logs.map((log, idx) => {
                const isError = log.level === 'ERROR' || log.message.toLowerCase().includes('error') || log.message.toLowerCase().includes('fail')
                return (
                  <div key={idx} className={`log-entry ${isError ? 'error' : ''}`}>
                    <span className="log-timestamp">
                      [{new Date(log.timestamp * 1000).toLocaleTimeString([], { hour12: false })}]
                    </span>
                    <span className="log-service">
                      {log.service.toUpperCase()}:
                    </span>
                    <span className="log-msg">
                      {log.message}
                    </span>
                  </div>
                )
              })
            ) : (
              <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
                <span style={{ color: 'var(--text-dim)', fontStyle: 'italic' }}>Listening for cluster logs...</span>
              </div>
            )}
          </div>
        </div>

        <div className="glass logs-card" style={{ padding: '24px', background: 'rgba(124, 58, 237, 0.03)', border: '1px solid rgba(124, 58, 237, 0.1)' }}>
          <h3 style={{ color: 'var(--color-primary)' }}>
            <MessageSquareCode size={18} />
            <span>System Details</span>
          </h3>
          <div style={{ fontSize: '13px', lineHeight: '1.6', color: 'var(--text-muted)', display: 'flex', flexDirection: 'column', gap: '8px', fontFamily: 'var(--font-main)' }}>
            <p>
              <strong>Load Balancing:</strong> Nginx routes <code>/api/users</code> traffic round-robin between User Service A and B.
            </p>
            <p>
              <strong>Caching Layer:</strong> Redis caches User profiles for 60 seconds. Killing Redis drops cache rate to 0%.
            </p>
            <p>
              <strong>Kafka Backpressure:</strong> Creating orders produces events to Kafka. Notification consumer lag spikes if the consumer delays.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
