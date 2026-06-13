import React from 'react'
import { Server, Activity, AlertTriangle, Cpu, HardDrive, Database } from 'lucide-react'

export default function ServiceCard({ service }) {
  const { name, status, latency_ms, error_rate, cpu_pct, memory_mb, cache_hit_ratio } = service

  // Format display name
  const displayName = name
    .replace('-1', ' (Replica A)')
    .replace('-2', ' (Replica B)')
    .split('-')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')

  const isOnline = status === 'online'
  const isDegraded = status === 'degraded'
  const isOffline = status === 'offline'

  const getStatusClass = () => {
    if (isOnline) return 'online'
    if (isDegraded) return 'degraded'
    return 'offline'
  }

  return (
    <div className={`glass service-card ${getStatusClass()}`}>
      <div className="card-header">
        <div className="card-title-group" style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <Server size={20} className={isOnline ? 'metric-value cyan' : isDegraded ? 'metric-value yellow' : 'metric-value red'} />
          <div>
            <h2 style={{ fontSize: '16px', fontWeight: '700' }}>{displayName}</h2>
            <span>{name}</span>
          </div>
        </div>
        <div className={`status-badge ${getStatusClass()}`}>
          {status}
        </div>
      </div>

      <div className="metrics-row">
        <div className="metric-box">
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <Activity size={12} className="text-muted" />
            <div className="metric-label">Latency</div>
          </div>
          <div className="metric-value cyan">
            {isOffline ? '—' : `${latency_ms} ms`}
          </div>
        </div>

        <div className="metric-box">
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <AlertTriangle size={12} className="text-muted" />
            <div className="metric-label">Error Rate</div>
          </div>
          <div className={`metric-value ${error_rate > 0 ? 'red' : 'text-main'}`}>
            {isOffline ? '—' : `${error_rate}%`}
          </div>
        </div>
      </div>

      {cache_hit_ratio !== undefined && cache_hit_ratio !== null && (
        <div className="metric-box" style={{ width: '100%' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <Database size={12} className="text-muted" />
            <div className="metric-label">Redis Cache Hit Rate</div>
          </div>
          <div className="metric-value purple">
            {isOffline ? '—' : `${cache_hit_ratio}%`}
          </div>
        </div>
      )}

      <div className="resource-grid">
        <div className="resource-bar-container">
          <div className="resource-bar-header">
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <Cpu size={11} />
              <span>CPU</span>
            </div>
            <span>{isOffline ? '0%' : `${cpu_pct}%`}</span>
          </div>
          <div className="bar-track">
            <div 
              className="bar-fill purple" 
              style={{ width: isOffline ? '0%' : `${Math.min(100, cpu_pct)}%` }}
            />
          </div>
        </div>

        <div className="resource-bar-container">
          <div className="resource-bar-header">
            <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
              <HardDrive size={11} />
              <span>MEM</span>
            </div>
            <span>{isOffline ? '0 MB' : `${memory_mb} MB`}</span>
          </div>
          <div className="bar-track">
            <div 
              className="bar-fill cyan" 
              style={{ width: isOffline ? '0%' : `${Math.min(100, (memory_mb / 512) * 100)}%` }}
            />
          </div>
        </div>
      </div>
    </div>
  )
}
