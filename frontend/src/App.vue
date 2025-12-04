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
      <Sidebar 
        :collapsed="sidebarCollapsed"
        :activeView="activeView"
        :stats="stats"
        :isRefreshing="isRefreshing"
        :worksheetCount="worksheetCount"
        @toggle-collapse="sidebarCollapsed = !sidebarCollapsed"
        @navigate="activeView = $event"
        @refresh="fetchAll"
      />

      <!-- Main Content Area -->
      <main class="main-content">
        <div class="view-container">
          <!-- Worksheets View (new multi-worksheet page) -->
          <WorksheetsPage 
            v-if="activeView === 'worksheets'" 
            :databases="databases"
            @query-executed="handleQueryExecuted"
            @worksheet-count-changed="worksheetCount = $event"
            @navigate="activeView = $event"
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

          <!-- Logs View -->
          <LogsPage 
            v-if="activeView === 'logs'" 
          />
          
          <!-- Database Explorer View (new stacked browser) -->
          <ObjectBrowserNew
            v-if="activeView === 'databases'"
            @open-worksheet="openInWorksheet"
            @preview-data="previewData"
          />
          
          <!-- Data Import View -->
          <DataImport
            v-if="activeView === 'import'"
            @import-complete="handleImportComplete"
          />

          <!-- Stages Browser -->
          <ObjectBrowser
            v-if="activeView === 'stages'"
            type="stages"
            title="Stages"
            icon="📦"
            :objects="stages"
            @refresh="fetchStages"
          />

          <!-- Warehouses View -->
          <div v-if="activeView === 'warehouses'" class="placeholder-view">
            <h2>⚡ Warehouses</h2>
            <div class="info-card">
              <h3>COMPUTE_WH</h3>
              <p>Status: <span class="status-running">RUNNING</span></p>
              <p>Size: X-SMALL</p>
              <p>Type: STANDARD</p>
            </div>
            <p class="note">Snowglobe uses local compute resources. Warehouses are simulated for compatibility.</p>
          </div>

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
import Sidebar from './components/Sidebar.vue'
import WorksheetsPage from './components/WorksheetsPage.vue'
import OverviewPanel from './components/OverviewPanel.vue'
import QueryHistoryPanel from './components/QueryHistoryPanel.vue'
import SessionsPanel from './components/SessionsPanel.vue'
import DatabaseExplorer from './components/DatabaseExplorer.vue'
import ObjectBrowser from './components/ObjectBrowser.vue'
import ObjectBrowserNew from './components/ObjectBrowserNew.vue'
import SettingsPanel from './components/SettingsPanel.vue'
import LogsPage from './components/LogsPage.vue'
import DataImport from './components/DataImport.vue'

export default {
  name: 'App',
  components: {
    Sidebar,
    WorksheetsPage,
    OverviewPanel,
    QueryHistoryPanel,
    SessionsPanel,
    DatabaseExplorer,
    ObjectBrowser,
    ObjectBrowserNew,
    SettingsPanel,
    LogsPage,
    DataImport
  },
  data() {
    return {
      activeView: 'worksheets',
      sidebarCollapsed: false,
      serverStatus: {},
      stats: {},
      sessions: [],
      queries: [],
      databases: [],
      schemas: [],
      tables: [],
      views: [],
      stages: [],
      worksheetCount: 1,
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
        this.fetchDatabases(),
        this.fetchSchemas(),
        this.fetchTables(),
        this.fetchViews()
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
    async fetchSchemas() {
      try {
        // Fetch schemas from current database via execute
        const response = await axios.post('/api/execute', {
          sql: 'SHOW SCHEMAS'
        })
        if (response.data.success) {
          this.schemas = response.data.data.map(row => ({
            name: row[0],
            created_at: row[1]
          }))
        }
      } catch (error) {
        console.error('Failed to fetch schemas:', error)
      }
    },
    async fetchTables() {
      try {
        const response = await axios.post('/api/execute', {
          sql: 'SHOW TABLES'
        })
        if (response.data.success) {
          this.tables = response.data.data.map(row => ({
            name: row[0],
            created_at: row[1],
            row_count: row[2]
          }))
        }
      } catch (error) {
        console.error('Failed to fetch tables:', error)
      }
    },
    async fetchViews() {
      try {
        const response = await axios.post('/api/execute', {
          sql: 'SHOW VIEWS'
        })
        if (response.data.success) {
          this.views = response.data.data.map(row => ({
            name: row[0],
            created_at: row[1]
          }))
        }
      } catch (error) {
        console.error('Failed to fetch views:', error)
      }
    },
    async fetchStages() {
      // Stages are simulated
      this.stages = []
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
      this.fetchTables()
      this.fetchViews()
    },
    async previewTableData(table) {
      this.activeView = 'worksheets'
      // Could pass query to worksheet
    },
    async describeTable(table) {
      this.activeView = 'worksheets'
      // Could pass describe query to worksheet
    },
    drillDownSchema(schema) {
      // Navigate to tables filtered by schema
      this.activeView = 'tables'
    },
    openInWorksheet(data) {
      // Navigate to worksheets and create new worksheet with SQL
      this.activeView = 'worksheets'
      // The WorksheetsPage will handle the new worksheet creation via event
      this.$nextTick(() => {
        // This could be improved with a proper store/bus
        console.log('Open in worksheet:', data)
      })
    },
    previewData(data) {
      // Navigate to worksheets and execute preview query
      this.activeView = 'worksheets'
      this.$nextTick(() => {
        console.log('Preview data:', data)
      })
    },
    handleImportComplete(result) {
      // Refresh data after import
      this.fetchAll()
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

.placeholder-view {
  padding: 1.5rem;
}

.placeholder-view h2 {
  font-size: 1.5rem;
  margin: 0 0 1.5rem 0;
}

.info-card {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 1.5rem;
  max-width: 400px;
}

.info-card h3 {
  margin: 0 0 1rem 0;
  font-size: 1.1rem;
}

.info-card p {
  margin: 0.5rem 0;
  font-size: 0.9rem;
}

.status-running {
  color: #10b981;
  font-weight: 500;
}

.note {
  margin-top: 1.5rem;
  font-size: 0.85rem;
  color: var(--text-muted);
  font-style: italic;
}
</style>
