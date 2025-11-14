<template>
  <div class="overview">
    <h2 class="section-title">Server Overview</h2>
    
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-icon">⏱️</div>
        <div class="stat-content">
          <div class="stat-value">{{ formatUptime(stats.uptime_seconds) }}</div>
          <div class="stat-label">Uptime</div>
        </div>
      </div>
      
      <div class="stat-card">
        <div class="stat-icon">🔗</div>
        <div class="stat-content">
          <div class="stat-value">{{ stats.active_sessions || 0 }}</div>
          <div class="stat-label">Active Sessions</div>
        </div>
      </div>
      
      <div class="stat-card">
        <div class="stat-icon">📝</div>
        <div class="stat-content">
          <div class="stat-value">{{ stats.total_queries || 0 }}</div>
          <div class="stat-label">Total Queries</div>
        </div>
      </div>
      
      <div class="stat-card success">
        <div class="stat-icon">✅</div>
        <div class="stat-content">
          <div class="stat-value">{{ stats.successful_queries || 0 }}</div>
          <div class="stat-label">Successful</div>
        </div>
      </div>
      
      <div class="stat-card error">
        <div class="stat-icon">❌</div>
        <div class="stat-content">
          <div class="stat-value">{{ stats.failed_queries || 0 }}</div>
          <div class="stat-label">Failed</div>
        </div>
      </div>
      
      <div class="stat-card">
        <div class="stat-icon">⚡</div>
        <div class="stat-content">
          <div class="stat-value">{{ formatDuration(stats.average_query_duration_ms) }}</div>
          <div class="stat-label">Avg Query Time</div>
        </div>
      </div>
    </div>

    <div class="charts-row">
      <div class="chart-card">
        <h3>Query Success Rate</h3>
        <div class="progress-ring">
          <svg viewBox="0 0 100 100">
            <circle cx="50" cy="50" r="40" class="ring-bg" />
            <circle 
              cx="50" 
              cy="50" 
              r="40" 
              class="ring-progress"
              :stroke-dasharray="`${successRate * 2.51} 251`"
            />
          </svg>
          <div class="ring-text">{{ successRate }}%</div>
        </div>
      </div>
      
      <div class="chart-card">
        <h3>Active Sessions</h3>
        <div class="sessions-list">
          <div v-if="sessions.length === 0" class="empty-state">
            No active sessions
          </div>
          <div v-for="session in sessions.slice(0, 5)" :key="session.session_id" class="session-item">
            <div class="session-user">👤 {{ session.user }}</div>
            <div class="session-details">
              <span>{{ session.database }}.{{ session.schema }}</span>
            </div>
          </div>
          <div v-if="sessions.length > 5" class="more-sessions">
            +{{ sessions.length - 5 }} more sessions
          </div>
        </div>
      </div>
    </div>

    <div class="server-info">
      <h3>Server Information</h3>
      <div class="info-grid">
        <div class="info-item">
          <span class="info-label">Started At:</span>
          <span class="info-value">{{ formatDateTime(stats.server_start_time) }}</span>
        </div>
        <div class="info-item">
          <span class="info-label">Data Directory:</span>
          <span class="info-value">/data</span>
        </div>
        <div class="info-item">
          <span class="info-label">Port:</span>
          <span class="info-value">8084</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'OverviewPanel',
  props: {
    stats: {
      type: Object,
      default: () => ({})
    },
    sessions: {
      type: Array,
      default: () => []
    }
  },
  computed: {
    successRate() {
      if (!this.stats.total_queries || this.stats.total_queries === 0) return 100
      return Math.round((this.stats.successful_queries / this.stats.total_queries) * 100)
    }
  },
  methods: {
    formatUptime(seconds) {
      if (!seconds) return '0s'
      const hours = Math.floor(seconds / 3600)
      const minutes = Math.floor((seconds % 3600) / 60)
      const secs = Math.floor(seconds % 60)
      if (hours > 0) {
        return `${hours}h ${minutes}m`
      } else if (minutes > 0) {
        return `${minutes}m ${secs}s`
      }
      return `${secs}s`
    },
    formatDuration(ms) {
      if (!ms) return '0ms'
      if (ms < 1) return '<1ms'
      if (ms < 1000) return `${Math.round(ms)}ms`
      return `${(ms / 1000).toFixed(2)}s`
    },
    formatDateTime(isoString) {
      if (!isoString) return 'N/A'
      return new Date(isoString).toLocaleString()
    }
  }
}
</script>

<style scoped>
.overview {
  display: flex;
  flex-direction: column;
  gap: 2rem;
}

.section-title {
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--text-color);
  margin-bottom: 0.5rem;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
}

.stat-card {
  background: var(--card-bg);
  padding: 1.5rem;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  display: flex;
  align-items: center;
  gap: 1rem;
  border: 1px solid var(--border-color);
}

.stat-card.success {
  border-left: 4px solid var(--success-color);
}

.stat-card.error {
  border-left: 4px solid var(--error-color);
}

.stat-icon {
  font-size: 2rem;
}

.stat-value {
  font-size: 1.75rem;
  font-weight: 700;
  color: var(--text-color);
}

.stat-label {
  font-size: 0.875rem;
  color: var(--text-muted);
}

.charts-row {
  display: grid;
  grid-template-columns: 1fr 2fr;
  gap: 1.5rem;
}

.chart-card {
  background: var(--card-bg);
  padding: 1.5rem;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  border: 1px solid var(--border-color);
}

.chart-card h3 {
  font-size: 1rem;
  font-weight: 600;
  margin-bottom: 1rem;
  color: var(--text-color);
}

.progress-ring {
  position: relative;
  width: 150px;
  height: 150px;
  margin: 0 auto;
}

.progress-ring svg {
  transform: rotate(-90deg);
}

.ring-bg {
  fill: none;
  stroke: var(--border-color);
  stroke-width: 10;
}

.ring-progress {
  fill: none;
  stroke: var(--success-color);
  stroke-width: 10;
  stroke-linecap: round;
  transition: stroke-dasharray 0.5s ease;
}

.ring-text {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  font-size: 2rem;
  font-weight: 700;
  color: var(--text-color);
}

.sessions-list {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.empty-state {
  color: var(--text-muted);
  text-align: center;
  padding: 2rem;
}

.session-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem;
  background: var(--bg-color);
  border-radius: 8px;
}

.session-user {
  font-weight: 500;
}

.session-details {
  font-size: 0.875rem;
  color: var(--text-muted);
}

.more-sessions {
  text-align: center;
  color: var(--text-muted);
  font-size: 0.875rem;
  padding: 0.5rem;
}

.server-info {
  background: var(--card-bg);
  padding: 1.5rem;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  border: 1px solid var(--border-color);
}

.server-info h3 {
  font-size: 1rem;
  font-weight: 600;
  margin-bottom: 1rem;
}

.info-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 1rem;
}

.info-item {
  display: flex;
  gap: 0.5rem;
}

.info-label {
  font-weight: 500;
  color: var(--text-muted);
}

.info-value {
  color: var(--text-color);
}
</style>
