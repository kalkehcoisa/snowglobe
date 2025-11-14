<template>
  <div class="app">
    <!-- Top Header -->
    <header class="header">
      <div class="header-content">
        <div class="logo">
          <span class="logo-icon">❄️</span>
          <h1>Snowglobe</h1>
        </div>
        <div class="header-status">
          <span :class="['status-badge', serverStatus.status === 'healthy' ? 'status-healthy' : 'status-error']">
            {{ serverStatus.status || 'connecting...' }}
          </span>
          <span class="version">v{{ serverStatus.version || '0.1.0' }}</span>
          <span v-if="isHttps" class="https-badge">🔒 HTTPS</span>
        </div>
      </div>
    </header>

    <div class="app-container">
      <!-- Side Navigation Menu -->
      <aside class="sidebar">
        <nav class="sidebar-nav">
          <button 
            v-for="item in navItems" 
            :key="item.id"
            :class="['nav-item', { active: activeView === item.id }]"
            @click="activeView = item.id"
          >
            <span class="nav-icon">{{ item.icon }}</span>
            <span class="nav-label">{{ item.label }}</span>
          </button>
        </nav>

        <div class="sidebar-footer">
          <div class="connection-info">
            <div class="info-label">Active Sessions</div>
            <div class="info-value">{{ stats.active_sessions || 0 }}</div>
          </div>
          <div class="connection-info">
            <div class="info-label">Queries</div>
            <div class="info-value">{{ stats.total_queries || 0 }}</div>
          </div>
          <button class="refresh-btn" @click="fetchAll" title="Refresh">
            <span :class="{ 'rotating': isRefreshing }">🔄</span>
          </button>
        </div>
      </aside>

      <!-- Main Content Area -->
      <main class="main-content">
        <div class="view-container">
          <!-- Worksheet View -->
          <WorksheetPanel 
            v-if="activeView === 'worksheet'" 
            @query-executed="handleQueryExecuted"
          />

          <!-- Overview View -->
          <OverviewPanel 
            v-if="activeView === 'overview'" 
            :stats="stats" 
            :sessions="sessions" 
          />
          
          <!-- Query History View -->
          <QueryHistoryPanel 
            v-if="activeView === 'queries'" 
            :queries="queries" 
            @refresh="fetchQueries" 
            @clear="clearHistory" 
          />
          
          <!-- Sessions View -->
          <SessionsPanel 
            v-if="activeView === 'sessions'" 
            :sessions="sessions" 
            @refresh="fetchSessions" 
          />
          
          <!-- Database Explorer View -->
          <DatabaseExplorer 
            v-if="activeView === 'databases'" 
            :databases="databases" 
            @refresh="fetchDatabases" 
          />

          <!-- Settings View -->
          <SettingsPanel 
            v-if="activeView === 'settings'" 
            :serverStatus="serverStatus"
            :stats="stats"
            @toggle-auto-refresh="autoRefresh = !autoRefresh"
            :auto-refresh="autoRefresh"
          />
        </div>
      </main>
    </div>

    <!-- Footer Status Bar -->
    <footer class="footer">
      <div class="footer-content">
        <span>⏱️ Uptime: {{ formatUptime(stats.uptime_seconds) }}</span>
        <span>•</span>
        <span>📊 Queries: {{ stats.total_queries || 0 }} (✅ {{ stats.successful_queries || 0 }} / ❌ {{ stats.failed_queries || 0 }})</span>
        <span>•</span>
        <span>⚡ Avg: {{ Math.round(stats.average_query_duration_ms || 0) }}ms</span>
        <span>•</span>
        <span>{{ autoRefresh ? '▶️ Auto-refresh ON' : '⏸️ Auto-refresh OFF' }}</span>
      </div>
    </footer>
  </div>
</template>

<script>
import axios from 'axios'
import WorksheetPanel from './components/WorksheetPanel.vue'
import OverviewPanel from './components/OverviewPanel.vue'
import QueryHistoryPanel from './components/QueryHistoryPanel.vue'
import SessionsPanel from './components/SessionsPanel.vue'
import DatabaseExplorer from './components/DatabaseExplorer.vue'
import SettingsPanel from './components/SettingsPanel.vue'

export default {
  name: 'App',
  components: {
    WorksheetPanel,
    OverviewPanel,
    QueryHistoryPanel,
    SessionsPanel,
    DatabaseExplorer,
    SettingsPanel
  },
  data() {
    return {
      activeView: 'worksheet',
      navItems: [
        { id: 'worksheet', label: 'Worksheet', icon: '📝' },
        { id: 'overview', label: 'Overview', icon: '📊' },
        { id: 'queries', label: 'Query History', icon: '🕒' },
        { id: 'sessions', label: 'Sessions', icon: '🔗' },
        { id: 'databases', label: 'Databases', icon: '🗄️' },
        { id: 'settings', label: 'Settings', icon: '⚙️' }
      ],
      serverStatus: {},
      stats: {},
      sessions: [],
      queries: [],
      databases: [],
      autoRefresh: true,
      refreshInterval: null,
      isRefreshing: false,
      isHttps: window.location.protocol === 'https:'
    }
  },
  mounted() {
    this.fetchAll()
    this.startAutoRefresh()
  },
  beforeUnmount() {
    this.stopAutoRefresh()
  },
  methods: {
    async fetchAll() {
      this.isRefreshing = true
      await Promise.all([
        this.fetchHealth(),
        this.fetchStats(),
        this.fetchSessions(),
        this.fetchQueries(),
        this.fetchDatabases()
      ])
      this.isRefreshing = false
    },
    async fetchHealth() {
      try {
        const response = await axios.get('/health')
        this.serverStatus = response.data
      } catch (error) {
        this.serverStatus = { status: 'error', version: 'unknown' }
      }
    },
    async fetchStats() {
      try {
        const response = await axios.get('/api/stats')
        this.stats = response.data
      } catch (error) {
        console.error('Failed to fetch stats:', error)
      }
    },
    async fetchSessions() {
      try {
        const response = await axios.get('/api/sessions')
        this.sessions = response.data.sessions
      } catch (error) {
        console.error('Failed to fetch sessions:', error)
      }
    },
    async fetchQueries() {
      try {
        const response = await axios.get('/api/queries?limit=100')
        this.queries = response.data.queries
      } catch (error) {
        console.error('Failed to fetch queries:', error)
      }
    },
    async fetchDatabases() {
      try {
        const response = await axios.get('/api/databases')
        this.databases = response.data.databases
      } catch (error) {
        console.error('Failed to fetch databases:', error)
      }
    },
    async clearHistory() {
      try {
        await axios.delete('/api/queries/history')
        this.queries = []
        this.fetchStats()
      } catch (error) {
        console.error('Failed to clear history:', error)
      }
    },
    handleQueryExecuted() {
      // Refresh stats and queries after a query is executed
      this.fetchStats()
      this.fetchQueries()
    },
    startAutoRefresh() {
      this.refreshInterval = setInterval(() => {
        if (this.autoRefresh) {
          this.fetchAll()
        }
      }, 5000)
    },
    stopAutoRefresh() {
      if (this.refreshInterval) {
        clearInterval(this.refreshInterval)
      }
    },
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
    }
  }
}
</script>

<style scoped>
.app {
  display: flex;
  flex-direction: column;
  height: 100vh;
  background: var(--bg-color);
}

.header {
  background: linear-gradient(135deg, #29b5e8, #1aa3d9);
  color: white;
  padding: 0.75rem 1.5rem;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  z-index: 100;
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.logo {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.logo-icon {
  font-size: 1.75rem;
}

.logo h1 {
  font-size: 1.35rem;
  font-weight: 600;
  margin: 0;
}

.header-status {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.status-badge {
  padding: 0.25rem 0.75rem;
  border-radius: 1rem;
  font-size: 0.8rem;
  font-weight: 500;
  text-transform: uppercase;
}

.status-healthy {
  background: rgba(16, 185, 129, 0.3);
  border: 1px solid rgba(16, 185, 129, 0.6);
}

.status-error {
  background: rgba(239, 68, 68, 0.3);
  border: 1px solid rgba(239, 68, 68, 0.6);
}

.https-badge {
  padding: 0.25rem 0.75rem;
  border-radius: 1rem;
  font-size: 0.8rem;
  font-weight: 500;
  background: rgba(16, 185, 129, 0.3);
  border: 1px solid rgba(16, 185, 129, 0.6);
}

.version {
  opacity: 0.9;
  font-size: 0.8rem;
}

.app-container {
  display: flex;
  flex: 1;
  overflow: hidden;
}

.sidebar {
  width: 240px;
  background: var(--card-bg);
  border-right: 1px solid var(--border-color);
  display: flex;
  flex-direction: column;
  overflow-y: auto;
}

.sidebar-nav {
  flex: 1;
  padding: 0.5rem;
}

.nav-item {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.75rem 1rem;
  background: none;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  color: var(--text-muted);
  font-size: 0.95rem;
  transition: all 0.2s ease;
  margin-bottom: 0.25rem;
  text-align: left;
}

.nav-item:hover {
  background: var(--bg-color);
  color: var(--text-color);
}

.nav-item.active {
  background: linear-gradient(135deg, rgba(41, 181, 232, 0.15), rgba(26, 163, 217, 0.15));
  color: var(--primary-color);
  font-weight: 500;
}

.nav-icon {
  font-size: 1.25rem;
  width: 24px;
  text-align: center;
}

.nav-label {
  flex: 1;
}

.sidebar-footer {
  padding: 1rem;
  border-top: 1px solid var(--border-color);
  background: var(--bg-color);
}

.connection-info {
  display: flex;
  justify-content: space-between;
  margin-bottom: 0.5rem;
  font-size: 0.85rem;
}

.info-label {
  color: var(--text-muted);
}

.info-value {
  font-weight: 600;
  color: var(--primary-color);
}

.refresh-btn {
  width: 100%;
  padding: 0.5rem;
  background: var(--primary-color);
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 1rem;
  margin-top: 0.5rem;
  transition: all 0.2s ease;
}

.refresh-btn:hover {
  opacity: 0.9;
  transform: translateY(-1px);
}

.rotating {
  display: inline-block;
  animation: rotate 1s linear infinite;
}

@keyframes rotate {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.main-content {
  flex: 1;
  overflow-y: auto;
  background: var(--bg-color);
}

.view-container {
  padding: 1.5rem;
  max-width: 1600px;
  margin: 0 auto;
  animation: fadeIn 0.3s ease;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

.footer {
  background: var(--card-bg);
  border-top: 1px solid var(--border-color);
  padding: 0.5rem 1.5rem;
  z-index: 100;
}

.footer-content {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  font-size: 0.8rem;
  color: var(--text-muted);
  flex-wrap: wrap;
}
</style>
