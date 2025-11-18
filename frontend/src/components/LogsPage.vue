<template>
  <div class="logs-page">
    <div class="logs-header">
      <div class="header-title">
        <h2>📋 Server Logs</h2>
        <span class="log-count">{{ logs.length }} / {{ totalLogs }} entries</span>
      </div>
      <div class="header-controls">
        <div class="filter-group">
          <label>Level:</label>
          <select v-model="selectedLevel" @change="fetchLogs">
            <option value="">All</option>
            <option value="DEBUG">DEBUG</option>
            <option value="INFO">INFO</option>
            <option value="WARNING">WARNING</option>
            <option value="ERROR">ERROR</option>
            <option value="CRITICAL">CRITICAL</option>
          </select>
        </div>
        <div class="filter-group">
          <label>Limit:</label>
          <select v-model="limit" @change="fetchLogs">
            <option :value="50">50</option>
            <option :value="100">100</option>
            <option :value="200">200</option>
            <option :value="500">500</option>
          </select>
        </div>
        <button class="btn-refresh" @click="fetchLogs" :disabled="isLoading">
          <span :class="{ rotating: isLoading }">🔄</span>
          Refresh
        </button>
        <button class="btn-clear" @click="clearLogs" :disabled="logs.length === 0">
          🗑️ Clear
        </button>
        <label class="auto-refresh-toggle">
          <input type="checkbox" v-model="autoRefresh" @change="toggleAutoRefresh" />
          Auto-refresh
        </label>
      </div>
    </div>

    <div class="logs-container">
      <div v-if="isLoading && logs.length === 0" class="loading">
        Loading logs...
      </div>
      
      <div v-else-if="logs.length === 0" class="empty-state">
        <span class="empty-icon">📋</span>
        <p>No logs found</p>
        <p class="hint">Server logs will appear here as activity occurs</p>
      </div>
      
      <div v-else class="logs-list">
        <div 
          v-for="(log, index) in logs" 
          :key="index"
          :class="['log-entry', `level-${log.level.toLowerCase()}`]"
        >
          <div class="log-meta">
            <span :class="['log-level', `level-${log.level.toLowerCase()}`]">
              {{ log.level }}
            </span>
            <span class="log-timestamp">{{ formatTimestamp(log.timestamp) }}</span>
            <span class="log-source">{{ log.logger }}</span>
          </div>
          <div class="log-message">{{ log.message }}</div>
          <div class="log-location" v-if="showDetails">
            {{ log.module }}.{{ log.function }}:{{ log.line }}
          </div>
        </div>
      </div>
    </div>

    <div class="logs-footer">
      <label class="show-details">
        <input type="checkbox" v-model="showDetails" />
        Show source details
      </label>
      <span class="footer-info">
        Showing {{ logs.length }} of {{ totalLogs }} log entries
      </span>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'LogsPage',
  data() {
    return {
      logs: [],
      totalLogs: 0,
      selectedLevel: '',
      limit: 100,
      isLoading: false,
      autoRefresh: false,
      refreshInterval: null,
      showDetails: false
    }
  },
  mounted() {
    this.fetchLogs()
  },
  beforeUnmount() {
    this.stopAutoRefresh()
  },
  methods: {
    async fetchLogs() {
      this.isLoading = true
      try {
        const params = { limit: this.limit }
        if (this.selectedLevel) {
          params.level = this.selectedLevel
        }
        const response = await axios.get('/api/logs', { params })
        this.logs = response.data.logs
        this.totalLogs = response.data.total
      } catch (error) {
        console.error('Failed to fetch logs:', error)
      } finally {
        this.isLoading = false
      }
    },
    
    async clearLogs() {
      if (!confirm('Clear all server logs?')) return
      
      try {
        await axios.delete('/api/logs')
        this.logs = []
        this.totalLogs = 0
      } catch (error) {
        console.error('Failed to clear logs:', error)
      }
    },
    
    toggleAutoRefresh() {
      if (this.autoRefresh) {
        this.startAutoRefresh()
      } else {
        this.stopAutoRefresh()
      }
    },
    
    startAutoRefresh() {
      this.refreshInterval = setInterval(() => {
        this.fetchLogs()
      }, 3000)
    },
    
    stopAutoRefresh() {
      if (this.refreshInterval) {
        clearInterval(this.refreshInterval)
        this.refreshInterval = null
      }
    },
    
    formatTimestamp(timestamp) {
      const date = new Date(timestamp)
      return date.toLocaleTimeString() + '.' + String(date.getMilliseconds()).padStart(3, '0')
    }
  }
}
</script>

<style scoped>
.logs-page {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--bg-color);
}

.logs-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem;
  background: var(--card-bg);
  border-bottom: 1px solid var(--border-color);
  flex-wrap: wrap;
  gap: 1rem;
}

.header-title {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.header-title h2 {
  font-size: 1.25rem;
  margin: 0;
}

.log-count {
  font-size: 0.85rem;
  color: var(--text-muted);
  background: var(--bg-color);
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
}

.header-controls {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  flex-wrap: wrap;
}

.filter-group {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.filter-group label {
  font-size: 0.85rem;
  color: var(--text-muted);
}

.filter-group select {
  padding: 0.375rem 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--bg-color);
  font-size: 0.85rem;
}

.btn-refresh,
.btn-clear {
  padding: 0.5rem 0.75rem;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.85rem;
  display: flex;
  align-items: center;
  gap: 0.375rem;
  background: var(--card-bg);
}

.btn-refresh:hover,
.btn-clear:hover {
  background: var(--bg-color);
}

.btn-clear {
  color: #ef4444;
  border-color: #ef4444;
}

.btn-clear:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.auto-refresh-toggle {
  display: flex;
  align-items: center;
  gap: 0.375rem;
  font-size: 0.85rem;
  cursor: pointer;
}

.logs-container {
  flex: 1;
  overflow-y: auto;
  padding: 1rem;
}

.loading,
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 200px;
  color: var(--text-muted);
}

.empty-icon {
  font-size: 3rem;
  margin-bottom: 1rem;
}

.empty-state .hint {
  font-size: 0.85rem;
  margin-top: 0.5rem;
}

.logs-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.log-entry {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 0.75rem;
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 0.8rem;
}

.log-entry.level-debug {
  border-left: 3px solid #6b7280;
}

.log-entry.level-info {
  border-left: 3px solid #3b82f6;
}

.log-entry.level-warning {
  border-left: 3px solid #f59e0b;
}

.log-entry.level-error {
  border-left: 3px solid #ef4444;
  background: rgba(239, 68, 68, 0.05);
}

.log-entry.level-critical {
  border-left: 3px solid #dc2626;
  background: rgba(220, 38, 38, 0.1);
}

.log-meta {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  margin-bottom: 0.5rem;
}

.log-level {
  padding: 0.125rem 0.375rem;
  border-radius: 3px;
  font-size: 0.7rem;
  font-weight: 600;
}

.log-level.level-debug {
  background: #6b7280;
  color: white;
}

.log-level.level-info {
  background: #3b82f6;
  color: white;
}

.log-level.level-warning {
  background: #f59e0b;
  color: white;
}

.log-level.level-error {
  background: #ef4444;
  color: white;
}

.log-level.level-critical {
  background: #dc2626;
  color: white;
}

.log-timestamp {
  color: var(--text-muted);
  font-size: 0.75rem;
}

.log-source {
  color: var(--primary-color);
  font-size: 0.75rem;
}

.log-message {
  word-break: break-word;
  white-space: pre-wrap;
}

.log-location {
  margin-top: 0.5rem;
  font-size: 0.7rem;
  color: var(--text-muted);
}

.logs-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem 1rem;
  background: var(--card-bg);
  border-top: 1px solid var(--border-color);
  font-size: 0.85rem;
}

.show-details {
  display: flex;
  align-items: center;
  gap: 0.375rem;
  cursor: pointer;
}

.footer-info {
  color: var(--text-muted);
}

.rotating {
  display: inline-block;
  animation: rotate 1s linear infinite;
}

@keyframes rotate {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
</style>
