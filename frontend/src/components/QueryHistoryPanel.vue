<template>
  <div class="query-history">
    <div class="panel-header">
      <h2 class="section-title">Query History</h2>
      <div class="header-actions">
        <button class="action-button" @click="$emit('refresh')">
          🔄 Refresh
        </button>
        <button class="action-button danger" @click="$emit('clear')">
          🗑️ Clear History
        </button>
      </div>
    </div>

    <div class="filters">
      <input 
        v-model="searchQuery" 
        type="text" 
        placeholder="Search queries..." 
        class="search-input"
      />
      <select v-model="statusFilter" class="status-filter">
        <option value="all">All Status</option>
        <option value="success">Success</option>
        <option value="failed">Failed</option>
      </select>
    </div>

    <div class="query-list">
      <div v-if="filteredQueries.length === 0" class="empty-state">
        <div class="empty-icon">📝</div>
        <p>No queries found</p>
      </div>

      <div 
        v-for="query in filteredQueries" 
        :key="query.id" 
        class="query-item"
        :class="{ 'query-failed': !query.success }"
        @click="selectedQuery = query"
      >
        <div class="query-header">
          <div class="query-status">
            <span :class="['status-dot', query.success ? 'success' : 'error']"></span>
            {{ query.success ? 'Success' : 'Failed' }}
          </div>
          <div class="query-time">{{ formatTimestamp(query.timestamp) }}</div>
        </div>
        
        <div class="query-sql">
          {{ truncateSQL(query.query) }}
        </div>
        
        <div class="query-meta">
          <span>⏱️ {{ formatDuration(query.duration_ms) }}</span>
          <span>📊 {{ query.rows_affected }} rows</span>
          <span>🔗 {{ query.session_id.substring(0, 8) }}...</span>
        </div>

        <div v-if="query.error" class="query-error">
          ❌ {{ query.error }}
        </div>
      </div>
    </div>

    <!-- Query Detail Modal -->
    <div v-if="selectedQuery" class="modal-overlay" @click.self="selectedQuery = null">
      <div class="modal-content">
        <div class="modal-header">
          <h3>Query Details</h3>
          <button class="close-button" @click="selectedQuery = null">×</button>
        </div>
        <div class="modal-body">
          <div class="detail-row">
            <span class="detail-label">Status:</span>
            <span :class="['status-badge', selectedQuery.success ? 'success' : 'error']">
              {{ selectedQuery.success ? 'Success' : 'Failed' }}
            </span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Timestamp:</span>
            <span>{{ formatTimestamp(selectedQuery.timestamp) }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Duration:</span>
            <span>{{ formatDuration(selectedQuery.duration_ms) }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Rows Affected:</span>
            <span>{{ selectedQuery.rows_affected }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Session ID:</span>
            <span class="monospace">{{ selectedQuery.session_id }}</span>
          </div>
          <div class="sql-block">
            <span class="detail-label">SQL Query:</span>
            <pre class="sql-code">{{ selectedQuery.query }}</pre>
          </div>
          <div v-if="selectedQuery.error" class="error-block">
            <span class="detail-label">Error:</span>
            <pre class="error-message">{{ selectedQuery.error }}</pre>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'QueryHistoryPanel',
  props: {
    queries: {
      type: Array,
      default: () => []
    }
  },
  data() {
    return {
      searchQuery: '',
      statusFilter: 'all',
      selectedQuery: null
    }
  },
  computed: {
    filteredQueries() {
      return this.queries.filter(query => {
        // Status filter
        if (this.statusFilter === 'success' && !query.success) return false
        if (this.statusFilter === 'failed' && query.success) return false
        
        // Search filter
        if (this.searchQuery) {
          const search = this.searchQuery.toLowerCase()
          return query.query.toLowerCase().includes(search) ||
                 (query.error && query.error.toLowerCase().includes(search))
        }
        
        return true
      })
    }
  },
  methods: {
    formatTimestamp(isoString) {
      if (!isoString) return 'N/A'
      const date = new Date(isoString)
      return date.toLocaleTimeString() + ' ' + date.toLocaleDateString()
    },
    formatDuration(ms) {
      if (!ms) return '0ms'
      if (ms < 1) return '<1ms'
      if (ms < 1000) return `${Math.round(ms)}ms`
      return `${(ms / 1000).toFixed(2)}s`
    },
    truncateSQL(sql) {
      if (!sql) return ''
      const maxLength = 200
      if (sql.length <= maxLength) return sql
      return sql.substring(0, maxLength) + '...'
    }
  }
}
</script>

<style scoped>
.query-history {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.section-title {
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--text-color);
}

.header-actions {
  display: flex;
  gap: 0.75rem;
}

.action-button {
  padding: 0.5rem 1rem;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: var(--card-bg);
  cursor: pointer;
  font-size: 0.875rem;
  transition: all 0.2s ease;
}

.action-button:hover {
  background: var(--bg-color);
}

.action-button.danger:hover {
  background: #fee2e2;
  border-color: var(--error-color);
}

.filters {
  display: flex;
  gap: 1rem;
}

.search-input {
  flex: 1;
  padding: 0.75rem 1rem;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  font-size: 0.95rem;
}

.search-input:focus {
  outline: none;
  border-color: var(--primary-color);
  box-shadow: 0 0 0 3px rgba(41, 181, 232, 0.1);
}

.status-filter {
  padding: 0.75rem 1rem;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: var(--card-bg);
  font-size: 0.95rem;
}

.query-list {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.empty-state {
  text-align: center;
  padding: 3rem;
  color: var(--text-muted);
}

.empty-icon {
  font-size: 3rem;
  margin-bottom: 1rem;
}

.query-item {
  background: var(--card-bg);
  padding: 1rem 1.25rem;
  border-radius: 10px;
  border: 1px solid var(--border-color);
  cursor: pointer;
  transition: all 0.2s ease;
}

.query-item:hover {
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
  transform: translateY(-1px);
}

.query-item.query-failed {
  border-left: 4px solid var(--error-color);
}

.query-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.75rem;
}

.query-status {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.875rem;
  font-weight: 500;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.status-dot.success {
  background: var(--success-color);
}

.status-dot.error {
  background: var(--error-color);
}

.query-time {
  font-size: 0.8rem;
  color: var(--text-muted);
}

.query-sql {
  font-family: 'Fira Code', 'Consolas', monospace;
  font-size: 0.9rem;
  background: var(--bg-color);
  padding: 0.75rem;
  border-radius: 6px;
  margin-bottom: 0.75rem;
  word-break: break-word;
  line-height: 1.5;
}

.query-meta {
  display: flex;
  gap: 1.5rem;
  font-size: 0.8rem;
  color: var(--text-muted);
}

.query-error {
  margin-top: 0.75rem;
  padding: 0.5rem;
  background: #fee2e2;
  color: var(--error-color);
  border-radius: 6px;
  font-size: 0.85rem;
}

/* Modal Styles */
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-content {
  background: var(--card-bg);
  border-radius: 12px;
  max-width: 700px;
  width: 90%;
  max-height: 80vh;
  overflow-y: auto;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1.25rem 1.5rem;
  border-bottom: 1px solid var(--border-color);
}

.modal-header h3 {
  font-size: 1.1rem;
  font-weight: 600;
}

.close-button {
  background: none;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  color: var(--text-muted);
}

.modal-body {
  padding: 1.5rem;
}

.detail-row {
  display: flex;
  gap: 1rem;
  margin-bottom: 0.75rem;
}

.detail-label {
  font-weight: 500;
  color: var(--text-muted);
  min-width: 120px;
}

.status-badge {
  padding: 0.25rem 0.75rem;
  border-radius: 1rem;
  font-size: 0.8rem;
  font-weight: 500;
}

.status-badge.success {
  background: #d1fae5;
  color: #065f46;
}

.status-badge.error {
  background: #fee2e2;
  color: #991b1b;
}

.monospace {
  font-family: 'Fira Code', 'Consolas', monospace;
  font-size: 0.9rem;
}

.sql-block, .error-block {
  margin-top: 1rem;
}

.sql-code {
  font-family: 'Fira Code', 'Consolas', monospace;
  font-size: 0.9rem;
  background: var(--bg-color);
  padding: 1rem;
  border-radius: 8px;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
}

.error-message {
  font-family: 'Fira Code', 'Consolas', monospace;
  font-size: 0.9rem;
  background: #fee2e2;
  color: #991b1b;
  padding: 1rem;
  border-radius: 8px;
  white-space: pre-wrap;
  word-break: break-word;
}
</style>
