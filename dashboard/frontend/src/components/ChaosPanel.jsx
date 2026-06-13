import React, { useState, useEffect } from 'react'
import { Flame, Play, Clock, CheckCircle2, ShieldAlert, Trash2 } from 'lucide-react'

export default function ChaosPanel({ activeChaos }) {
  const [targetSelections, setTargetSelections] = useState({
    kill_container: 'user-service-1',
    cpu_spike: 'user-service-1',
    network_delay: 'user-service-1'
  })
  
  const [schedulerConfig, setSchedulerConfig] = useState({
    scenario: 'kill_container',
    target: 'user-service-1',
    interval: 30
  })

  const [scheduleList, setScheduleList] = useState([])
  const [triggering, setTriggering] = useState(null)
  const [clearing, setClearing] = useState(false)

  // Targets list
  const containerTargets = ['user-service-1', 'user-service-2', 'order-service', 'notif-service']

  // Scenarios descriptions
  const scenarios = [
    {
      id: 'kill_container',
      title: 'Kill Container',
      desc: 'Simulate container crash. Triggers auto-restart.',
      hasTarget: true
    },
    {
      id: 'cpu_spike',
      title: 'Spike CPU',
      desc: 'Inject stress-ng loop to spike CPU to 100%.',
      hasTarget: true
    },
    {
      id: 'network_delay',
      title: 'Network Delay (500ms)',
      desc: 'Add latency queue using tc netem to retard requests.',
      hasTarget: true
    },
    {
      id: 'kafka_flood',
      title: 'Flood Kafka Queue',
      desc: 'Produce 10k messages rapidly to trigger consumer lag.',
      hasTarget: false
    },
    {
      id: 'db_corrupt',
      title: 'Corrupt DB (Pause Postgres)',
      desc: 'Pause SQL primary container to block query pool.',
      hasTarget: false
    }
  ]

  // Fetch current schedules
  const fetchSchedules = async () => {
    try {
      const res = await fetch('/api/chaos/schedule')
      if (res.ok) {
        const data = await res.json()
        setScheduleList(data)
      }
    } catch (e) {
      console.error('Failed to fetch schedules:', e)
    }
  }

  useEffect(() => {
    fetchSchedules()
    const interval = setInterval(fetchSchedules, 5000)
    return () => clearInterval(interval)
  }, [])

  // Trigger chaos event
  const triggerChaos = async (scenarioId, hasTarget) => {
    const target = hasTarget ? targetSelections[scenarioId] : null
    const label = hasTarget ? `${scenarioId} on ${target}` : scenarioId
    setTriggering(label)
    
    try {
      await fetch('/api/chaos/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          scenario: scenarioId,
          target: target,
          duration: 30
        })
      })
    } catch (e) {
      console.error('Failed to trigger chaos:', e)
    } finally {
      setTimeout(() => setTriggering(null), 1000)
    }
  }

  // Clear all chaos
  const clearAllChaos = async () => {
    setClearing(true)
    try {
      await fetch('/api/chaos/clear', { method: 'POST' })
    } catch (e) {
      console.error('Failed to clear chaos:', e)
    } finally {
      setTimeout(() => setClearing(false), 1000)
    }
  }

  // Schedule chaos
  const addSchedule = async () => {
    const targetRequired = ['kill_container', 'cpu_spike', 'network_delay'].includes(schedulerConfig.scenario)
    try {
      await fetch('/api/chaos/schedule', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          scenario: schedulerConfig.scenario,
          target: targetRequired ? schedulerConfig.target : '',
          interval: parseInt(schedulerConfig.interval)
        })
      })
      fetchSchedules()
    } catch (e) {
      console.error('Failed to add schedule:', e)
    }
  }

  // Clear schedule jobs
  const clearSchedules = async () => {
    try {
      await fetch('/api/chaos/schedule/clear', { method: 'POST' })
      setScheduleList([])
    } catch (e) {
      console.error('Failed to clear schedules:', e)
    }
  }

  // Check if scenario is active
  const isScenarioActive = (scenarioId, target) => {
    if (!activeChaos) return false
    
    if (scenarioId === 'kafka_flood') {
      return !!activeChaos['kafka']?.['kafka_flood']
    }
    if (scenarioId === 'db_corrupt') {
      return !!activeChaos['postgres']?.['db_corrupt']
    }
    
    // Check specific container
    return !!activeChaos[target]?.[scenarioId]
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      <div className="glass chaos-panel">
        <h2>
          <Flame size={22} className="metric-value red animate-pulse" />
          <span>Chaos Control Board</span>
        </h2>

        <div className="chaos-buttons-grid">
          {scenarios.map(sc => {
            const currentTarget = sc.hasTarget ? targetSelections[sc.id] : null
            const active = isScenarioActive(sc.id, currentTarget)
            const loading = triggering === (sc.hasTarget ? `${sc.id} on ${currentTarget}` : sc.id)

            return (
              <div key={sc.id} className={`chaos-btn ${active ? 'active' : ''}`}>
                <div className="chaos-btn-details">
                  <div className="chaos-btn-title">
                    {sc.title}
                    {active && <span className="status-badge offline" style={{ fontSize: '9px', padding: '1px 5px' }}>ACTIVE</span>}
                  </div>
                  <div className="chaos-btn-desc">{sc.desc}</div>
                  
                  {sc.hasTarget && (
                    <div style={{ marginTop: '8px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Target:</span>
                      <select 
                        className="target-select"
                        value={targetSelections[sc.id]}
                        onChange={(e) => setTargetSelections({
                          ...targetSelections,
                          [sc.id]: e.target.value
                        })}
                        disabled={active}
                      >
                        {containerTargets.map(t => (
                          <option key={t} value={t}>{t}</option>
                        ))}
                      </select>
                    </div>
                  )}
                </div>

                <button 
                  className="chaos-btn-trigger" 
                  disabled={loading}
                  onClick={() => triggerChaos(sc.id, sc.hasTarget)}
                  style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
                >
                  <Play size={12} fill="currentColor" />
                  {loading ? 'Triggering...' : 'Fire'}
                </button>
              </div>
            )
          })}
        </div>

        <button 
          className="clear-all-btn" 
          onClick={clearAllChaos}
          disabled={clearing}
        >
          <CheckCircle2 size={16} />
          {clearing ? 'Healing Systems...' : 'Clear All Chaos / Heal'}
        </button>
      </div>

      <div className="glass chaos-panel" style={{ background: 'rgba(15, 10, 30, 0.4)' }}>
        <h2 style={{ color: 'var(--color-primary)' }}>
          <Clock size={20} className="metric-value purple" />
          <span>Automated Chaos Scheduler</span>
        </h2>

        <div className="scheduler-section" style={{ border: 'none', paddingTop: 0 }}>
          <div className="scheduler-controls">
            <select 
              className="target-select"
              style={{ padding: '8px', borderRadius: '8px' }}
              value={schedulerConfig.scenario}
              onChange={(e) => setSchedulerConfig({ ...schedulerConfig, scenario: e.target.value })}
            >
              <option value="kill_container">Kill Container</option>
              <option value="cpu_spike">Spike CPU</option>
              <option value="network_delay">Network Delay</option>
              <option value="kafka_flood">Flood Kafka</option>
              <option value="db_corrupt">Pause Postgres</option>
            </select>

            {['kill_container', 'cpu_spike', 'network_delay'].includes(schedulerConfig.scenario) ? (
              <select 
                className="target-select"
                style={{ padding: '8px', borderRadius: '8px' }}
                value={schedulerConfig.target}
                onChange={(e) => setSchedulerConfig({ ...schedulerConfig, target: e.target.value })}
              >
                {containerTargets.map(t => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            ) : (
              <div style={{ background: 'rgba(255,255,255,0.02)', border: '1px solid rgba(255,255,255,0.05)', borderRadius: '8px' }} />
            )}

            <select
              className="target-select"
              style={{ padding: '8px', borderRadius: '8px' }}
              value={schedulerConfig.interval}
              onChange={(e) => setSchedulerConfig({ ...schedulerConfig, interval: e.target.value })}
            >
              <option value="15">15s</option>
              <option value="30">30s</option>
              <option value="60">60s</option>
            </select>
          </div>

          <button className="schedule-btn" onClick={addSchedule} style={{ padding: '10px', borderRadius: '8px' }}>
            Schedule Automation
          </button>
        </div>

        {scheduleList.length > 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div className="scheduler-title">
                <ShieldAlert size={12} />
                <span>Active Automation Cycles ({scheduleList.length})</span>
              </div>
              <button 
                onClick={clearSchedules} 
                style={{ background: 'none', border: 'none', color: 'var(--color-red)', fontSize: '11px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px' }}
              >
                <Trash2 size={10} /> Clear
              </button>
            </div>

            <div className="scheduled-jobs-list">
              {scheduleList.map((job, idx) => (
                <div key={idx} className="scheduled-job-item">
                  <span>
                    {job.scenario.toUpperCase()} {job.target ? `[${job.target}]` : ''} every {job.interval}s
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
