<template>
  <div class="settings-panel">
    <div class="panel-header">
      <h2>⚙️ Settings</h2>
    </div>

    <div class="settings-grid">
      <!-- Server Information -->
      <div class="setting-section">
        <h3>🖥️ Server Information</h3>
        <div class="setting-group">
          <div class="setting-item">
            <span class="setting-label">Status:</span>
            <span :class="['setting-value', statusClass]">
              {{ serverStatus.status || 'Unknown' }}
            </span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Version:</span>
            <span class="setting-value">{{ serverStatus.version || 'Unknown' }}</span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Protocol:</span>
            <span class="setting-value">
              {{ isHttps ? '🔒 HTTPS (Secure)' : '🔓 HTTP' }}
            </span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Uptime:</span>
            <span class="setting-value">{{ stats.uptime_formatted || 'N/A' }}</span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Started:</span>
            <span class="setting-value">{{ formatDate(stats.server_start_time) }}</span>
          </div>
        </div>
      </div>

      <!-- Connection Settings -->
      <div class="setting-section">
        <h3>🔗 Connection</h3>
        <div class="setting-group">
          <div class="setting-item">
            <span class="setting-label">Endpoint:</span>
            <span class="setting-value code">{{ currentEndpoint }}</span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Active Sessions:</span>
            <span class="setting-value badge">{{ stats.active_sessions || 0 }}</span>
          </div>
          <div class="setting-item">
            <span class="setting-label">SSL/TLS:</span>
            <span class="setting-value">{{ isHttps ? 'Enabled ✅' : 'Disabled ⚠️' }}</span>
          </div>
        </div>
        <div class="info-box">
          <strong>💡 Connection String Example:</strong>
          <pre class="code-block">import snowflake.connector

conn = snowflake.connector.connect(
    account='localhost',
    user='dev',
    password='dev',
    host='localhost',
    port={{ isHttps ? '8443' : '8084' }},
    protocol='{{ isHttps ? 'https' : 'http' }}',
    database='TEST_DB',
    schema='PUBLIC'
)</pre>
        </div>
      </div>

      <!-- Performance Metrics -->
      <div class="setting-section">
        <h3>📊 Performance Metrics</h3>
        <div class="setting-group">
          <div class="setting-item">
            <span class="setting-label">Total Queries:</span>
            <span class="setting-value badge">{{ stats.total_queries || 0 }}</span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Successful:</span>
            <span class="setting-value success">{{ stats.successful_queries || 0 }}</span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Failed:</span>
            <span class="setting-value error">{{ stats.failed_queries || 0 }}</span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Avg Duration:</span>
            <span class="setting-value">{{ Math.round(stats.average_query_duration_ms || 0) }}ms</span>
          </div>
          <div class="setting-item">
            <span class="setting-label">Success Rate:</span>
            <span class="setting-value">{{ successRate }}%</span>
          </div>
        </div>
      </div>

      <!-- UI Settings -->
      <div class="setting-section">
        <h3>🎨 User Interface</h3>
        <div class="setting-group">
          <div class="setting-control">
            <label class="toggle-label">
              <input 
                type="checkbox" 
                :checked="autoRefresh" 
                @change="$emit('toggle-auto-refresh')"
              >
              <span class="toggle-slider"></span>
              <span class="toggle-text">Auto-refresh Dashboard (5s interval)</span>
            </label>
          </div>
        </div>
      </div>

      <!-- Security Information -->
      <div class="setting-section">
        <h3>🔐 Security</h3>
        <div class="info-box" :class="isHttps ? 'info-success' : 'info-warning'">
          <div v-if="isHttps">
            <strong>✅ HTTPS Enabled</strong>
            <p>Your connection is encrypted using SSL/TLS. This is the recommended configuration for Snowflake compatibility.</p>
            <ul>
              <li>Encrypted data transmission</li>
              <li>Snowflake standard protocol</li>
              <li>Self-signed certificate for development</li>
            </ul>
          </div>
          <div v-else>
            <strong>⚠️ HTTP Mode</strong>
            <p>You're connected via unencrypted HTTP. For production-like testing, enable HTTPS:</p>
            <ul>
              <li>Set <code>SNOWGLOBE_ENABLE_HTTPS=true</code></li>
              <li>Ensure SSL certificates are in <code>/app/certs/</code></li>
              <li>Connect to port 8443 instead of 8084</li>
            </ul>
          </div>
        </div>
      </div>

      <!-- Environment Variables -->
      <div class="setting-section full-width">
        <h3>📝 Environment Configuration</h3>
        <div class="info-box">
          <p><strong>Docker Environment Variables:</strong></p>
          <table class="env-table">
            <tr>
              <td><code>SNOWGLOBE_PORT</code></td>
              <td>HTTP port (default: 8084)</td>
            </tr>
            <tr>
              <td><code>SNOWGLOBE_HTTPS_PORT</code></td>
              <td>HTTPS port (default: 8443)</td>
            </tr>
            <tr>
              <td><code>SNOWGLOBE_ENABLE_HTTPS</code></td>
              <td>Enable HTTPS (default: true)</td>
            </tr>
            <tr>
              <td><code>SNOWGLOBE_DATA_DIR</code></td>
              <td>Data directory (default: /data)</td>
            </tr>
            <tr>
              <td><code>SNOWGLOBE_CERT_PATH</code></td>
              <td>SSL certificate path</td>
            </tr>
            <tr>
              <td><code>SNOWGLOBE_KEY_PATH</code></td>
              <td>SSL private key path</td>
            </tr>
          </table>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'SettingsPanel',
  props: {
    serverStatus: {
      type: Object,
      default: () => ({})
    },
    stats: {
      type: Object,
      default: () => ({})
    },
    autoRefresh: {
      type: Boolean,
      default: true
    }
  },
  computed: {
    isHttps() {
      return window.location.protocol === 'https:'
    },
    currentEndpoint() {
      return window.location.origin
    },
    statusClass() {
      return this.serverStatus.status === 'healthy' ? 'status-healthy' : 'status-error'
    },
    successRate() {
      const total = this.stats.total_queries || 0
      if (total === 0) return '100'
      const successful = this.stats.successful_queries || 0
      return ((successful / total) * 100).toFixed(1)
    }
  },
  methods: {
    formatDate(isoString) {
      if (!isoString) return 'N/A'
      try {
        return new Date(isoString).toLocaleString()
      } catch {
        return 'N/A'
      }
    }
  }
}
</script>

<style scoped>
.settings-panel {
  max-width: 1200px;
}

.panel-header {
  margin-bottom: 2rem;
}

.panel-header h2 {
  font-size: 1.75rem;
  margin: 0;
  color: var(--text-color);
}

.settings-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
  gap: 1.5rem;
}

.setting-section {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 1.5rem;
}

.setting-section.full-width {
  grid-column: 1 / -1;
}

.setting-section h3 {
  font-size: 1.1rem;
  margin: 0 0 1rem 0;
  color: var(--text-color);
}

.setting-group {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.setting-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.5rem 0;
  border-bottom: 1px solid var(--border-color);
}

.setting-item:last-child {
  border-bottom: none;
}

.setting-label {
  color: var(--text-muted);
  font-size: 0.9rem;
}

.setting-value {
  font-weight: 500;
  color: var(--text-color);
}

.setting-value.code {
  font-family: monospace;
  font-size: 0.85rem;
  background: var(--bg-color);
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
}

.setting-value.badge {
  background: var(--primary-color);
  color: white;
  padding: 0.25rem 0.75rem;
  border-radius: 12px;
  font-size: 0.85rem;
}

.setting-value.success {
  color: var(--success-color);
  font-weight: 600;
}

.setting-value.error {
  color: var(--error-color);
  font-weight: 600;
}

.status-healthy {
  color: var(--success-color);
  font-weight: 600;
}

.status-error {
  color: var(--error-color);
  font-weight: 600;
}

.setting-control {
  padding: 0.75rem 0;
}

.toggle-label {
  display: flex;
  align-items: center;
  gap: 1rem;
  cursor: pointer;
  user-select: none;
}

.toggle-label input[type="checkbox"] {
  display: none;
}

.toggle-slider {
  position: relative;
  width: 50px;
  height: 26px;
  background: var(--border-color);
  border-radius: 13px;
  transition: background 0.3s ease;
}

.toggle-slider::after {
  content: '';
  position: absolute;
  width: 20px;
  height: 20px;
  border-radius: 50%;
  background: white;
  top: 3px;
  left: 3px;
  transition: transform 0.3s ease;
}

.toggle-label input[type="checkbox"]:checked + .toggle-slider {
  background: var(--primary-color);
}

.toggle-label input[type="checkbox"]:checked + .toggle-slider::after {
  transform: translateX(24px);
}

.toggle-text {
  font-size: 0.95rem;
}

.info-box {
  background: var(--bg-color);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 1rem;
  margin-top: 1rem;
  font-size: 0.9rem;
}

.info-box.info-success {
  background: rgba(16, 185, 129, 0.1);
  border-color: var(--success-color);
}

.info-box.info-warning {
  background: rgba(251, 191, 36, 0.1);
  border-color: #fbbf24;
}

.info-box strong {
  display: block;
  margin-bottom: 0.5rem;
}

.info-box p {
  margin: 0.5rem 0;
  color: var(--text-muted);
}

.info-box ul {
  margin: 0.5rem 0;
  padding-left: 1.5rem;
  color: var(--text-muted);
}

.info-box li {
  margin: 0.25rem 0;
}

.code-block {
  background: #1e1e1e;
  color: #d4d4d4;
  padding: 1rem;
  border-radius: 4px;
  overflow-x: auto;
  margin: 0.5rem 0 0 0;
  font-size: 0.85rem;
  line-height: 1.5;
}

.env-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 0.5rem;
}

.env-table td {
  padding: 0.5rem;
  border-bottom: 1px solid var(--border-color);
}

.env-table td:first-child {
  font-family: monospace;
  color: var(--primary-color);
  font-weight: 500;
  width: 250px;
}

.env-table td:last-child {
  color: var(--text-muted);
}

code {
  background: var(--bg-color);
  padding: 0.125rem 0.375rem;
  border-radius: 3px;
  font-family: monospace;
  font-size: 0.875em;
}
</style>
