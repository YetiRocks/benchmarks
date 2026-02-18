import { useState, useEffect, useCallback, useRef } from 'react'

const BASE = '/benchmarks'

interface TestDef {
  id: string
  name: string
  binary: string
  duration: number
  vus: number
}

const TESTS: TestDef[] = [
  { id: 'rest-read', name: 'REST Reads', binary: 'throughput', duration: 30, vus: 50 },
  { id: 'rest-write', name: 'REST Writes', binary: 'throughput', duration: 30, vus: 50 },
  { id: 'rest-update', name: 'REST Update', binary: 'load-rest-crud', duration: 30, vus: 50 },
  { id: 'rest-join', name: 'REST Join', binary: 'load-rest-relationships', duration: 30, vus: 50 },
  { id: 'graphql-read', name: 'GraphQL Reads', binary: 'load-graphql', duration: 30, vus: 50 },
  { id: 'graphql-mutation', name: 'GraphQL Mutations', binary: 'load-graphql', duration: 30, vus: 50 },
  { id: 'vector-embed', name: 'Vector Embed', binary: 'load-vector-embed', duration: 30, vus: 50 },
  { id: 'vector-search', name: 'Vector Search', binary: 'load-vector-search', duration: 30, vus: 50 },
  { id: 'ws', name: 'WebSocket', binary: 'load-ws', duration: 30, vus: 50 },
  { id: 'sse', name: 'SSE Streaming', binary: 'load-sse', duration: 30, vus: 50 },
  { id: 'blob-retrieval', name: 'Blob Retrieval', binary: 'load-blob-storage', duration: 30, vus: 50 },
]

interface TestConfig {
  duration: number
  vus: number
}

interface LatestResult {
  name: string
  throughput: number
  run: Record<string, unknown>
  results: {
    throughput?: number
    p50?: number
    p99?: number
    total?: number
    errors?: number
    extrapolatedThroughput?: string
    summary?: string
  } | null
}

interface RunnerState {
  status: 'idle' | 'warming' | 'running'
  test?: string
  startedAt?: number
  warmupSecs?: number
  elapsedSecs?: number
  configuredDuration?: number
  lastResult?: unknown
  error?: string
}

interface HistoryRun {
  id: string
  testName: string
  timestamp: string
  durationSecs: number
  results: string
  summary: string
  extrapolatedThroughput: string
}

function formatNumber(n: number): string {
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M'
  if (n >= 1000) return (n / 1000).toFixed(n >= 10000 ? 0 : 1) + 'k'
  return n.toFixed(0)
}

function formatMs(n: number): string {
  if (n === 0) return '-'
  if (n < 1) return n.toFixed(3) + 'ms'
  return n.toFixed(2) + 'ms'
}

function formatDescription(cfg: TestConfig): string {
  return `${cfg.duration}s Duration. ${cfg.vus} VUs.`
}

export default function App() {
  const [latestResults, setLatestResults] = useState<Record<string, LatestResult>>({})
  const [configs, setConfigs] = useState<Record<string, TestConfig>>({})
  const [runner, setRunner] = useState<RunnerState>({ status: 'idle' })
  const [selectedTest, setSelectedTest] = useState<string | null>(null)
  const [history, setHistory] = useState<HistoryRun[]>([])
  const [error, setError] = useState<string | null>(null)
  const pollRef = useRef<number | null>(null)

  const getConfig = useCallback((testId: string): TestConfig => {
    if (configs[testId]) return configs[testId]
    const def = TESTS.find(t => t.id === testId)
    return def ? { duration: def.duration, vus: def.vus } : { duration: 60, vus: 50 }
  }, [configs])

  const fetchLatestResults = useCallback(async () => {
    try {
      const resp = await fetch(`${BASE}/best-results`)
      if (resp.ok) {
        const data = await resp.json()
        const map: Record<string, LatestResult> = {}
        for (const t of data.tests || []) {
          map[t.name] = t
        }
        setLatestResults(map)
      }
    } catch {
      // Server may not be ready yet
    }
  }, [])

  const fetchRunnerState = useCallback(async () => {
    try {
      const resp = await fetch(`${BASE}/runner`)
      if (resp.ok) {
        const data = await resp.json()
        const state: RunnerState = {
          status: data.status,
          test: data.testName,
          startedAt: data.startedAt,
          warmupSecs: data.warmupSecs,
          elapsedSecs: data.elapsedSecs,
          configuredDuration: data.configuredDuration,
          lastResult: data.lastResult,
          error: data.lastError,
        }
        setRunner(state)

        // Update configs from runner response
        if (data.configs) {
          const cfgMap: Record<string, TestConfig> = {}
          for (const c of data.configs) {
            cfgMap[c.id] = { duration: c.duration, vus: c.vus }
          }
          setConfigs(cfgMap)
        }

        if (state.status === 'idle' && pollRef.current) {
          clearInterval(pollRef.current)
          pollRef.current = null
          fetchLatestResults()
          if (selectedTest) fetchHistory(selectedTest)
          if (state.error) setError(state.error)
        }
      }
    } catch {
      // Server may not be ready yet
    }
  }, [fetchLatestResults, selectedTest])

  const fetchHistory = useCallback(async (testName: string) => {
    try {
      const resp = await fetch(`${BASE}/TestRun?testName==${testName}&limit=10`)
      if (resp.ok) {
        const data = await resp.json()
        const runs: HistoryRun[] = Array.isArray(data) ? data : data.data || []
        runs.sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''))
        setHistory(runs)
      }
    } catch {
      setHistory([])
    }
  }, [])

  useEffect(() => {
    fetchLatestResults()
    fetchRunnerState()
  }, [fetchLatestResults, fetchRunnerState])

  const startTest = async (testId: string) => {
    setError(null)
    try {
      const resp = await fetch(`${BASE}/runner`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ test: testId }),
      })
      if (resp.ok) {
        setRunner({ status: 'warming', test: testId, startedAt: Date.now() / 1000 })
        if (pollRef.current) clearInterval(pollRef.current)
        pollRef.current = window.setInterval(fetchRunnerState, 1000)
      } else {
        const text = await resp.text()
        setError(text || 'Failed to start test')
      }
    } catch (e) {
      setError(`Connection error: ${e}`)
    }
  }

  const saveConfig = async (testId: string, cfg: TestConfig) => {
    try {
      await fetch(`${BASE}/TestConfig`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: testId, duration: cfg.duration, vus: cfg.vus }),
      })
      setConfigs(prev => ({ ...prev, [testId]: cfg }))
    } catch {
      // silent
    }
  }

  const handleCardClick = (testId: string) => {
    setSelectedTest(testId === selectedTest ? null : testId)
    if (testId !== selectedTest) {
      fetchHistory(testId)
    }
  }

  const isBusy = runner.status === 'warming' || runner.status === 'running'
  const runningTest = runner.test

  return (
    <div>
      <header className="header">
        <div className="header-left">
          <img src="logo_white.svg" alt="Yeti" className="logo" />
        </div>
        <div className="header-title">Benchmarks</div>
      </header>

      <div className="app">
        {error && <div className="error-msg">{error}</div>}

        <div className="test-grid">
          {TESTS.map(test => {
            const isThisTest = runningTest === test.id
            return (
              <TestCard
                key={test.id}
                test={test}
                config={getConfig(test.id)}
                latest={latestResults[test.id]}
                phase={isThisTest ? runner.status : 'idle'}
                isDisabled={isBusy && !isThisTest}
                isSelected={selectedTest === test.id}
                warmupSecs={isThisTest ? (runner.warmupSecs ?? 0) : 0}
                elapsedSecs={isThisTest ? (runner.elapsedSecs ?? 0) : 0}
                configuredDuration={isThisTest ? (runner.configuredDuration ?? 0) : 0}
                onRun={() => startTest(test.id)}
                onClick={() => handleCardClick(test.id)}
                onSaveConfig={(cfg) => saveConfig(test.id, cfg)}
              />
            )
          })}
        </div>

        {selectedTest && (
          <HistoryPanel
            testName={selectedTest}
            runs={history}
          />
        )}
      </div>
    </div>
  )
}

interface TestCardProps {
  test: TestDef
  config: TestConfig
  latest?: LatestResult
  phase: 'idle' | 'warming' | 'running'
  isDisabled: boolean
  isSelected: boolean
  warmupSecs: number
  elapsedSecs: number
  configuredDuration: number
  onRun: () => void
  onClick: () => void
  onSaveConfig: (cfg: TestConfig) => void
}

function TestCard({ test, config, latest, phase, isDisabled, isSelected, warmupSecs, elapsedSecs, configuredDuration, onRun, onClick, onSaveConfig }: TestCardProps) {
  const results = latest?.results
  const hasData = results && results.throughput
  const [editing, setEditing] = useState(false)
  const [editDuration, setEditDuration] = useState(config.duration)
  const [editVus, setEditVus] = useState(config.vus)

  const isOverdue = phase === 'running' && configuredDuration > 0 && elapsedSecs > configuredDuration

  const startEdit = (e: React.MouseEvent) => {
    e.stopPropagation()
    setEditDuration(config.duration)
    setEditVus(config.vus)
    setEditing(true)
  }

  const cancelEdit = (e: React.MouseEvent) => {
    e.stopPropagation()
    setEditing(false)
  }

  const saveEdit = (e: React.MouseEvent) => {
    e.stopPropagation()
    onSaveConfig({ duration: editDuration, vus: editVus })
    setEditing(false)
  }

  const cardClass = [
    'test-card',
    phase === 'warming' ? 'warming' : '',
    phase === 'running' && !isOverdue ? 'running' : '',
    isOverdue ? 'overdue' : '',
    isSelected ? 'selected' : '',
  ].filter(Boolean).join(' ')

  return (
    <div
      className={cardClass}
      onClick={onClick}
      style={{ cursor: 'pointer' }}
    >
      <div className="card-title-row">
        <h3>{test.name}</h3>
        {phase === 'warming' ? (
          <div className="timer-box warming-box">
            <span className="spinner" />
            Warming {warmupSecs.toFixed(0)}s
          </div>
        ) : phase === 'running' ? (
          <div className={`timer-box ${isOverdue ? 'overdue-box' : ''}`}>
            <span className="spinner" />
            {elapsedSecs.toFixed(0)}s / {configuredDuration}s
          </div>
        ) : (
          <button
            className="run-btn"
            disabled={isDisabled}
            onClick={(e) => { e.stopPropagation(); onRun(); }}
          >
            Run
          </button>
        )}
      </div>
      <div className="card-config">
        {editing ? (
          <div className="config-editor" onClick={e => e.stopPropagation()}>
            <label>Duration: <input type="number" value={editDuration} onChange={e => setEditDuration(+e.target.value)} min={1} />s</label>
            <label>VUs: <input type="number" value={editVus} onChange={e => setEditVus(+e.target.value)} min={1} /></label>
            <div className="config-actions">
              <button className="config-save" onClick={saveEdit}>Save</button>
              <button className="config-cancel" onClick={cancelEdit}>Cancel</button>
            </div>
          </div>
        ) : (
          <div className="description" onClick={startEdit} title="Click to edit config">
            {formatDescription(config)}
          </div>
        )}
      </div>

      <div className="card-stats">
        <div className="stat">
          <div className="stat-label">Throughput</div>
          <div className="stat-value highlight">{hasData ? `${formatNumber(results.throughput!)} /s` : '—'}</div>
          {hasData && results.extrapolatedThroughput && (
            <div className="stat-sub">~{formatNumber(parseFloat(results.extrapolatedThroughput))} extrapolated</div>
          )}
        </div>
        <div className="stat">
          <div className="stat-label">Latency</div>
          <div className="stat-value">{hasData ? formatMs(results.p50 ?? 0) : '—'}</div>
          <div className="stat-sub">p99: {hasData ? formatMs(results.p99 ?? 0) : '—'}</div>
        </div>
      </div>
    </div>
  )
}

interface HistoryPanelProps {
  testName: string
  runs: HistoryRun[]
}

function HistoryPanel({ testName, runs }: HistoryPanelProps) {
  if (runs.length === 0) {
    return (
      <div className="history-section">
        <h3>History: {testName}</h3>
        <p className="no-data">No runs recorded yet</p>
      </div>
    )
  }

  return (
    <div className="history-section">
      <h3>History: {testName} ({runs.length} runs)</h3>
      <table className="history-table">
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Throughput</th>
            <th>Extrapolated</th>
            <th>Duration</th>
            <th>Summary</th>
          </tr>
        </thead>
        <tbody>
          {runs.map(run => {
            let parsed: Record<string, number> = {}
            try { parsed = JSON.parse(run.results || '{}') } catch { /* ignore */ }

            return (
              <tr key={run.id}>
                <td>{new Date(run.timestamp).toLocaleString()}</td>
                <td>{formatNumber(parsed.throughput ?? 0)} /s</td>
                <td>{run.extrapolatedThroughput ? formatNumber(parseFloat(run.extrapolatedThroughput)) + ' /s' : '-'}</td>
                <td>{run.durationSecs?.toFixed(1)}s</td>
                <td>{run.summary || '-'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
