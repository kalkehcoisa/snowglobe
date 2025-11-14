<template>
  <div class="app">
    <header class="header">
      <div class="header-content">
        <div class="logo">
          <span class="logo-icon">❄️</span>
          <h1>Snowglobe Dashboard</h1>
        </div>
        <div class="header-status">
          <span :class="['status-badge', serverStatus.status === 'healthy' ? 'status-healthy' : 'status-error']">
            {{ serverStatus.status || 'connecting...' }}
          </span>
          <span class="version">v{{ serverStatus.version || '0.1.0' }}</span>
        </div>
      </div>
    </header>

    <nav class="nav">
      <button 
        v-for="tab in tabs" 
        :key="tab.id"
        :class="['nav-button', { active: activeTab === tab.id }]"
        @click="activeTab = tab.id"
      >
        <span class="nav-icon">{{ tab.icon }}</span>
        {{ tab.name }}
      </button>
    </nav>

    <main class="main">
      <div v-if="activeTab === 'overview'" class="tab-content">
        <OverviewPanel :stats="stats" :sessions="sessions" />
      </div>
      
      <div v-if="activeTab === 'queries'" class="tab-content">
        <QueryHistoryPanel :queries="queries" @refresh="fetchQueries" @clear="clearHistory" />
      </div>
      
      <div v-if="activeTab === 'sessions'" class="tab-content">
        <SessionsPanel :sessions="sessions" @refresh="fetchSessions" />
      </div>
      
      <div v-if="activeTab === 'databases'" class="tab-content">
        <DatabaseExplorer :databases="databases" @refresh="fetchDatabases" />
      </div>
    </main>

    <footer class="footer">
      <div class="footer-content">
        <span>Uptime: {{ formatUptime(stats.uptime_seconds) }}</span>
        <span>•</span>
        <span>Active Sessions: {{ stats.active_sessions || 0 }}</span>
        <span>•</span>
        <span>Queries: {{ stats.total_queries || 0 }}</span>
        <span>•</span>
        <span>Auto-refresh: {{ autoRefresh ? 'ON' : 'OFF' }}</span>
        <button class="toggle-refresh" @click="autoRefresh = !autoRefresh">
          {{ autoRefresh ? '⏸️' : '▶️' }}
        </button>
      </div>
    </footer>
  </div>
</template>

<script>
import axios from 'axios'
import OverviewPanel from './components/OverviewPanel.vue'
import QueryHistoryPanel from './components/QueryHistoryPanel.vue'
import SessionsPanel from './components/SessionsPanel.vue'
import DatabaseExplorer from './components/DatabaseExplorer.vue'

export default {
  name: 'App',
  components: {
    OverviewPanel,
    QueryHistoryPanel,
    SessionsPanel,
    DatabaseExplorer
  },
  data() {
    return {
      activeTab: 'overview',
      tabs: [
        { id: 'overview', name: 'Overview', icon: '📊' },
        { id: 'queries', name: 'Query History', icon: '📝' },
        { id: 'sessions', name: 'Sessions', icon: '🔗' },
        { id: 'databases', name: 'Databases', icon: '🗄️' }
      ],
      serverStatus: {},
      stats: {},
      sessions: [],
      queries: [],
      databases: [],
      autoRefresh: true,
      refreshInterval: null
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
      await Promise.all([
        this.fetchHealth(),
        this.fetchStats(),
        this.fetchSessions(),
        this.fetchQueries(),
        this.fetchDatabases()
      ])
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
        return `${hours}h ${minutes}m ${secs}s`
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
  min-height: 100vh;
}

.header {
  background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
  color: white;
  padding: 1rem 2rem;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  max-width: 1400px;
  margin: 0 auto;
}

.logo {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.logo-icon {
  font-size: 2rem;
}

.logo h1 {
  font-size: 1.5rem;
  font-weight: 600;
}

.header-status {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.status-badge {
  padding: 0.25rem 0.75rem;
  border-radius: 1rem;
  font-size: 0.875rem;
  font-weight: 500;
}

.status-healthy {
  background: var(--success-color);
}

.status-error {
  background: var(--error-color);
}

.version {
  opacity: 0.8;
  font-size: 0.875rem;
}

.nav {
  background: var(--card-bg);
  border-bottom: 1px solid var(--border-color);
  padding: 0 2rem;
  display: flex;
  gap: 0.5rem;
  max-width: 1400px;
  margin: 0 auto;
  width: 100%;
}

.nav-button {
  background: none;
  border: none;
  padding: 1rem 1.5rem;
  cursor: pointer;
  font-size: 0.95rem;
  color: var(--text-muted);
  border-bottom: 3px solid transparent;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.nav-button:hover {
  color: var(--text-color);
  background: var(--bg-color);
}

.nav-button.active {
  color: var(--primary-color);
  border-bottom-color: var(--primary-color);
  font-weight: 500;
}

.nav-icon {
  font-size: 1.1rem;
}

.main {
  flex: 1;
  padding: 2rem;
  max-width: 1400px;
  margin: 0 auto;
  width: 100%;
}

.tab-content {
  animation: fadeIn 0.3s ease;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

.footer {
  background: var(--card-bg);
  border-top: 1px solid var(--border-color);
  padding: 1rem 2rem;
}

.footer-content {
  max-width: 1400px;
  margin: 0 auto;
  display: flex;
  align-items: center;
  gap: 0.75rem;
  font-size: 0.875rem;
  color: var(--text-muted);
}

.toggle-refresh {
  background: none;
  border: none;
  cursor: pointer;
  font-size: 1rem;
  padding: 0.25rem;
}
</style>
