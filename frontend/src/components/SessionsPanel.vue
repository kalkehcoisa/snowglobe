<template>
  <div class="sessions-panel">
    <div class="panel-header">
      <h2 class="section-title">Active Sessions</h2>
      <button class="action-button" @click="$emit('refresh')">
        🔄 Refresh
      </button>
    </div>

    <div class="sessions-grid">
      <div v-if="sessions.length === 0" class="empty-state">
        <div class="empty-icon">🔗</div>
        <p>No active sessions</p>
        <span class="empty-hint">Sessions will appear here when clients connect</span>
      </div>

      <div 
        v-for="session in sessions" 
        :key="session.session_id" 
        class="session-card"
      >
        <div class="session-header">
          <div class="user-info">
            <span class="user-icon">👤</span>
            <span class="user-name">{{ session.user }}</span>
          </div>
          <div class="session-badge active">Active</div>
        </div>

        <div class="session-details">
          <div class="detail-item">
            <span class="detail-icon">🗄️</span>
            <span class="detail-label">Database:</span>
            <span class="detail-value">{{ session.database }}</span>
          </div>
          
          <div class="detail-item">
            <span class="detail-icon">📁</span>
            <span class="detail-label">Schema:</span>
            <span class="detail-value">{{ session.schema }}</span>
          </div>
          
          <div class="detail-item">
            <span class="detail-icon">🏭</span>
            <span class="detail-label">Warehouse:</span>
            <span class="detail-value">{{ session.warehouse }}</span>
          </div>
          
          <div class="detail-item">
            <span class="detail-icon">👔</span>
            <span class="detail-label">Role:</span>
            <span class="detail-value">{{ session.role }}</span>
          </div>
        </div>

        <div class="session-footer">
          <div class="session-id">
            <span class="id-label">Session ID:</span>
            <span class="id-value">{{ session.session_id }}</span>
          </div>
          <div class="session-time">
            <span class="time-icon">⏰</span>
            Created: {{ formatDateTime(session.created_at) }}
          </div>
        </div>

        <div class="token-info">
          <span class="token-label">Token:</span>
          <span class="token-value">{{ session.token_prefix }}</span>
        </div>
      </div>
    </div>

    <div v-if="sessions.length > 0" class="sessions-summary">
      <div class="summary-item">
        <strong>{{ sessions.length }}</strong> active session(s)
      </div>
      <div class="summary-item">
        <strong>{{ uniqueUsers }}</strong> unique user(s)
      </div>
      <div class="summary-item">
        <strong>{{ uniqueDatabases }}</strong> database(s) in use
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'SessionsPanel',
  props: {
    sessions: {
      type: Array,
      default: () => []
    }
  },
  computed: {
    uniqueUsers() {
      const users = new Set(this.sessions.map(s => s.user))
      return users.size
    },
    uniqueDatabases() {
      const dbs = new Set(this.sessions.map(s => s.database))
      return dbs.size
    }
  },
  methods: {
    formatDateTime(isoString) {
      if (!isoString) return 'N/A'
      const date = new Date(isoString)
      return date.toLocaleString()
    }
  }
}
</script>

<style scoped>
.sessions-panel {
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

.sessions-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
  gap: 1.5rem;
}

.empty-state {
  text-align: center;
  padding: 4rem 2rem;
  background: var(--card-bg);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  grid-column: 1 / -1;
}

.empty-icon {
  font-size: 3rem;
  margin-bottom: 1rem;
}

.empty-state p {
  font-size: 1.1rem;
  color: var(--text-color);
  margin-bottom: 0.5rem;
}

.empty-hint {
  font-size: 0.9rem;
  color: var(--text-muted);
}

.session-card {
  background: var(--card-bg);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  padding: 1.5rem;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  transition: all 0.2s ease;
}

.session-card:hover {
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.1);
  transform: translateY(-2px);
}

.session-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1.25rem;
  padding-bottom: 1rem;
  border-bottom: 1px solid var(--border-color);
}

.user-info {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.user-icon {
  font-size: 1.5rem;
}

.user-name {
  font-size: 1.1rem;
  font-weight: 600;
  color: var(--text-color);
}

.session-badge {
  padding: 0.25rem 0.75rem;
  border-radius: 1rem;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
}

.session-badge.active {
  background: #d1fae5;
  color: #065f46;
}

.session-details {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.75rem;
  margin-bottom: 1.25rem;
}

.detail-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.9rem;
}

.detail-icon {
  font-size: 1rem;
}

.detail-label {
  color: var(--text-muted);
  font-weight: 500;
}

.detail-value {
  color: var(--text-color);
  font-family: 'Fira Code', 'Consolas', monospace;
}

.session-footer {
  background: var(--bg-color);
  padding: 1rem;
  border-radius: 8px;
  margin-bottom: 1rem;
}

.session-id {
  margin-bottom: 0.5rem;
}

.id-label {
  font-size: 0.8rem;
  color: var(--text-muted);
  display: block;
  margin-bottom: 0.25rem;
}

.id-value {
  font-family: 'Fira Code', 'Consolas', monospace;
  font-size: 0.85rem;
  color: var(--text-color);
  word-break: break-all;
}

.session-time {
  font-size: 0.8rem;
  color: var(--text-muted);
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.token-info {
  font-size: 0.8rem;
  color: var(--text-muted);
}

.token-label {
  font-weight: 500;
}

.token-value {
  font-family: 'Fira Code', 'Consolas', monospace;
}

.sessions-summary {
  background: var(--card-bg);
  padding: 1rem 1.5rem;
  border-radius: 10px;
  border: 1px solid var(--border-color);
  display: flex;
  justify-content: center;
  gap: 2rem;
}

.summary-item {
  font-size: 0.9rem;
  color: var(--text-muted);
}

.summary-item strong {
  color: var(--primary-color);
  font-size: 1.1rem;
}
</style>
