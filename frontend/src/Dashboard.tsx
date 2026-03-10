import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

const STORAGE_KEY = 'api_key'

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRateEntry {
  task: string
  avg_score: number
  attempts: number
}

interface ScoresResponse {
  status: 'idle' | 'loading' | 'success' | 'error'
  data: ScoreBucket[]
  error?: string
}

interface TimelineResponse {
  status: 'idle' | 'loading' | 'success' | 'error'
  data: TimelineEntry[]
  error?: string
}

interface PassRatesResponse {
  status: 'idle' | 'loading' | 'success' | 'error'
  data: PassRateEntry[]
  error?: string
}

const LAB_OPTIONS = [
  { value: 'lab-01', label: 'Lab 01' },
  { value: 'lab-02', label: 'Lab 02' },
  { value: 'lab-03', label: 'Lab 03' },
  { value: 'lab-04', label: 'Lab 04' },
  { value: 'lab-05', label: 'Lab 05' },
]

function Dashboard() {
  const [token] = useState(() => localStorage.getItem(STORAGE_KEY) ?? '')
  const [selectedLab, setSelectedLab] = useState('lab-05')

  const [scores, setScores] = useState<ScoresResponse>({
    status: 'idle',
    data: [],
  })
  const [timeline, setTimeline] = useState<TimelineResponse>({
    status: 'idle',
    data: [],
  })
  const [passRates, setPassRates] = useState<PassRatesResponse>({
    status: 'idle',
    data: [],
  })

  useEffect(() => {
    if (!token) return

    const controller = new AbortController()

    async function fetchData() {
      const headers = {
        Authorization: `Bearer ${token}`,
      }

      setScores({ status: 'loading', data: [] })
      setTimeline({ status: 'loading', data: [] })
      setPassRates({ status: 'loading', data: [] })

      try {
        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${selectedLab}`, {
            headers,
            signal: controller.signal,
          }),
          fetch(`/analytics/timeline?lab=${selectedLab}`, {
            headers,
            signal: controller.signal,
          }),
          fetch(`/analytics/pass-rates?lab=${selectedLab}`, {
            headers,
            signal: controller.signal,
          }),
        ])

        if (!scoresRes.ok) {
          throw new Error(`Scores: HTTP ${scoresRes.status}`)
        }
        if (!timelineRes.ok) {
          throw new Error(`Timeline: HTTP ${timelineRes.status}`)
        }
        if (!passRatesRes.ok) {
          throw new Error(`Pass rates: HTTP ${passRatesRes.status}`)
        }

        const scoresData: ScoreBucket[] = await scoresRes.json()
        const timelineData: TimelineEntry[] = await timelineRes.json()
        const passRatesData: PassRateEntry[] = await passRatesRes.json()

        setScores({ status: 'success', data: scoresData })
        setTimeline({ status: 'success', data: timelineData })
        setPassRates({ status: 'success', data: passRatesData })
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') {
          return
        }
        const message = err instanceof Error ? err.message : 'Unknown error'
        setScores((prev) => ({ ...prev, status: 'error', error: message }))
        setTimeline((prev) => ({ ...prev, status: 'error', error: message }))
        setPassRates((prev) => ({ ...prev, status: 'error', error: message }))
      }
    }

    fetchData()

    return () => controller.abort()
  }, [token, selectedLab])

  const barChartData = {
    labels: scores.data.map((s) => s.bucket),
    datasets: [
      {
        label: 'Number of Students',
        data: scores.data.map((s) => s.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  const lineChartData = {
    labels: timeline.data.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.data.map((t) => t.submissions),
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 2,
        tension: 0.3,
        fill: true,
      },
    ],
  }

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: true,
      },
    },
  }

  if (!token) {
    return (
      <div className="dashboard-container">
        <h1>Dashboard</h1>
        <p>Please enter your API key to view the dashboard.</p>
      </div>
    )
  }

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <h1>Analytics Dashboard</h1>
        <div className="lab-selector">
          <label htmlFor="lab-select">Select Lab: </label>
          <select
            id="lab-select"
            value={selectedLab}
            onChange={(e) => setSelectedLab(e.target.value)}
          >
            {LAB_OPTIONS.map((lab) => (
              <option key={lab.value} value={lab.value}>
                {lab.label}
              </option>
            ))}
          </select>
        </div>
      </header>

      <div className="dashboard-content">
        <section className="chart-section">
          <h2>Score Distribution</h2>
          {scores.status === 'loading' && <p>Loading...</p>}
          {scores.status === 'error' && <p>Error: {scores.error}</p>}
          {scores.status === 'success' && scores.data.length > 0 && (
            <div className="chart-container">
              <Bar data={barChartData} options={chartOptions} />
            </div>
          )}
          {scores.status === 'success' && scores.data.length === 0 && (
            <p>No score data available</p>
          )}
        </section>

        <section className="chart-section">
          <h2>Submissions Timeline</h2>
          {timeline.status === 'loading' && <p>Loading...</p>}
          {timeline.status === 'error' && <p>Error: {timeline.error}</p>}
          {timeline.status === 'success' && timeline.data.length > 0 && (
            <div className="chart-container">
              <Line data={lineChartData} options={chartOptions} />
            </div>
          )}
          {timeline.status === 'success' && timeline.data.length === 0 && (
            <p>No timeline data available</p>
          )}
        </section>

        <section className="table-section">
          <h2>Pass Rates per Task</h2>
          {passRates.status === 'loading' && <p>Loading...</p>}
          {passRates.status === 'error' && <p>Error: {passRates.error}</p>}
          {passRates.status === 'success' && passRates.data.length > 0 && (
            <table className="pass-rates-table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Average Score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {passRates.data.map((entry, index) => (
                  <tr key={index}>
                    <td>{entry.task}</td>
                    <td>{entry.avg_score.toFixed(1)}</td>
                    <td>{entry.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {passRates.status === 'success' && passRates.data.length === 0 && (
            <p>No pass rate data available</p>
          )}
        </section>
      </div>
    </div>
  )
}

export default Dashboard
