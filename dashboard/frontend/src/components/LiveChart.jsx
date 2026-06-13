import React from 'react'
import { Line } from 'react-chartjs-2'
import { Activity, AlertOctagon } from 'lucide-react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
} from 'chart.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
)

export default function LiveChart({ history }) {
  // Extract time labels
  const labels = history.map(h => {
    const date = new Date(h.timestamp * 1000)
    return date.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
  })

  // Extract service latencies
  const userA_latency = history.map(h => h.services?.['user-service-1']?.latency_ms ?? 0)
  const userB_latency = history.map(h => h.services?.['user-service-2']?.latency_ms ?? 0)
  const order_latency = history.map(h => h.services?.['order-service']?.latency_ms ?? 0)
  const notif_latency = history.map(h => h.services?.['notif-service']?.latency_ms ?? 0)

  // Extract service error rates
  const userA_errors = history.map(h => h.services?.['user-service-1']?.error_rate ?? 0)
  const userB_errors = history.map(h => h.services?.['user-service-2']?.error_rate ?? 0)
  const order_errors = history.map(h => h.services?.['order-service']?.error_rate ?? 0)
  const notif_errors = history.map(h => h.services?.['notif-service']?.error_rate ?? 0)

  // Common chart options
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 0 // Disable animation for pure real-time CPU performance rendering
    },
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: '#e5e7eb',
          font: {
            family: "'Outfit', sans-serif",
            size: 11
          },
          boxWidth: 10,
          usePointStyle: true
        }
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        backgroundColor: 'rgba(15, 10, 40, 0.95)',
        titleColor: '#a78bfa',
        bodyColor: '#f3f4f6',
        borderColor: 'rgba(167, 139, 250, 0.2)',
        borderWidth: 1
      }
    },
    scales: {
      x: {
        grid: {
          color: 'rgba(255, 255, 255, 0.03)'
        },
        ticks: {
          color: '#9ca3af',
          font: {
            family: "'JetBrains Mono', monospace",
            size: 9
          },
          maxTicksLimit: 8
        }
      },
      y: {
        grid: {
          color: 'rgba(255, 255, 255, 0.03)'
        },
        ticks: {
          color: '#9ca3af',
          font: {
            family: "'JetBrains Mono', monospace",
            size: 9
          }
        }
      }
    }
  }

  // Latency Chart Data
  const latencyData = {
    labels,
    datasets: [
      {
        label: 'User Service (Replica A)',
        data: userA_latency,
        borderColor: '#22d3ee',
        backgroundColor: 'rgba(34, 211, 238, 0.03)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.3,
        fill: true
      },
      {
        label: 'User Service (Replica B)',
        data: userB_latency,
        borderColor: '#a78bfa',
        backgroundColor: 'rgba(167, 139, 250, 0.03)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.3,
        fill: true
      },
      {
        label: 'Order Service',
        data: order_latency,
        borderColor: '#fbbf24',
        backgroundColor: 'rgba(251, 191, 36, 0.03)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.3,
        fill: true
      },
      {
        label: 'Notification Service',
        data: notif_latency,
        borderColor: '#f43f5e',
        backgroundColor: 'rgba(244, 63, 94, 0.03)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.3,
        fill: true
      }
    ]
  }

  // Error Rate Chart Data
  const errorData = {
    labels,
    datasets: [
      {
        label: 'User Service (Replica A)',
        data: userA_errors,
        borderColor: '#f87171',
        backgroundColor: 'rgba(248, 113, 113, 0.02)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.2
      },
      {
        label: 'User Service (Replica B)',
        data: userB_errors,
        borderColor: '#e879f9',
        backgroundColor: 'rgba(232, 121, 249, 0.02)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.2
      },
      {
        label: 'Order Service',
        data: order_errors,
        borderColor: '#fb7185',
        backgroundColor: 'rgba(251, 113, 133, 0.02)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.2
      },
      {
        label: 'Notification Service',
        data: notif_errors,
        borderColor: '#fdba74',
        backgroundColor: 'rgba(253, 186, 116, 0.02)',
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.2
      }
    ]
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      <div className="glass charts-panel">
        <h2>
          <Activity size={20} className="metric-value cyan" />
          <span>Real-time Response Latency (ms)</span>
        </h2>
        <div className="chart-wrapper">
          <Line data={latencyData} options={options} />
        </div>
      </div>

      <div className="glass charts-panel">
        <h2>
          <AlertOctagon size={20} className="metric-value red" />
          <span>Real-time Service Error Rate (%)</span>
        </h2>
        <div className="chart-wrapper">
          <Line data={errorData} options={options} />
        </div>
      </div>
    </div>
  )
}
