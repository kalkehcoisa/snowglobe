<template>
  <div class="database-explorer">
    <div class="panel-header">
      <h2 class="section-title">Database Explorer</h2>
      <button class="action-button" @click="$emit('refresh')">
        🔄 Refresh
      </button>
    </div>

    <div class="explorer-content">
      <div class="sidebar">
        <div class="sidebar-header">
          <h3>Databases</h3>
        </div>
        
        <div class="database-list">
          <div v-if="databases.length === 0" class="empty-state-small">
            No databases found
          </div>
          
          <div 
            v-for="db in databases" 
            :key="db"
            class="database-item"
            :class="{ selected: selectedDatabase === db }"
            @click="selectDatabase(db)"
          >
            <span class="db-icon">🗄️</span>
            <span class="db-name">{{ db }}</span>
            <span v-if="selectedDatabase === db" class="expand-icon">▶</span>
          </div>
        </div>
      </div>

      <div class="main-content">
        <div v-if="!selectedDatabase" class="placeholder">
          <div class="placeholder-icon">🗄️</div>
          <p>Select a database to view its schemas and tables</p>
        </div>

        <div v-else class="database-details">
          <div class="breadcrumb">
            <span class="breadcrumb-item" @click="clearSelection">Databases</span>
            <span class="breadcrumb-separator">/</span>
            <span class="breadcrumb-item current">{{ selectedDatabase }}</span>
            <span v-if="selectedSchema" class="breadcrumb-separator">/</span>
            <span v-if="selectedSchema" class="breadcrumb-item current">{{ selectedSchema }}</span>
          </div>

          <div v-if="loadingSchemas" class="loading">
            Loading schemas...
          </div>

          <div v-else-if="!selectedSchema" class="schemas-section">
            <h3>Schemas in {{ selectedDatabase }}</h3>
            <div class="schema-grid">
              <div v-if="schemas.length === 0" class="empty-state-small">
                No schemas found
              </div>
              <div 
                v-for="schema in schemas" 
                :key="schema"
                class="schema-card"
                @click="selectSchema(schema)"
              >
                <span class="schema-icon">📁</span>
                <span class="schema-name">{{ schema }}</span>
              </div>
            </div>
          </div>

          <div v-else class="tables-section">
            <div class="section-header">
              <h3>Tables in {{ selectedDatabase }}.{{ selectedSchema }}</h3>
              <button class="back-button" @click="selectedSchema = null">
                ← Back to Schemas
              </button>
            </div>

            <div v-if="loadingTables" class="loading">
              Loading tables...
            </div>

            <div v-else class="table-grid">
              <div v-if="tables.length === 0" class="empty-state-small">
                No tables found
              </div>
              <div 
                v-for="table in tables" 
                :key="table.name"
                class="table-card"
                @click="showTableDetails(table)"
              >
                <div class="table-header">
                  <span class="table-icon">📊</span>
                  <span class="table-name">{{ table.name }}</span>
                </div>
                <div class="table-meta">
                  <span class="table-type">{{ table.type }}</span>
                  <span class="table-created">{{ formatDate(table.created_at) }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Table Details Modal -->
    <div v-if="selectedTable" class="modal-overlay" @click.self="selectedTable = null">
      <div class="modal-content">
        <div class="modal-header">
          <h3>{{ selectedTable.name }}</h3>
          <button class="close-button" @click="selectedTable = null">×</button>
        </div>
        <div class="modal-body">
          <div class="detail-row">
            <span class="detail-label">Full Name:</span>
            <span class="monospace">{{ selectedDatabase }}.{{ selectedSchema }}.{{ selectedTable.name }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Type:</span>
            <span>{{ selectedTable.type }}</span>
          </div>
          <div class="detail-row">
            <span class="detail-label">Created At:</span>
            <span>{{ formatDateTime(selectedTable.created_at) }}</span>
          </div>
          <div v-if="selectedTable.columns" class="columns-section">
            <h4>Columns</h4>
            <table class="columns-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Nullable</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="col in selectedTable.columns" :key="col.name">
                  <td>{{ col.name }}</td>
                  <td class="monospace">{{ col.type }}</td>
                  <td>{{ col.nullable ? 'Yes' : 'No' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'DatabaseExplorer',
  props: {
    databases: {
      type: Array,
      default: () => []
    }
  },
  data() {
    return {
      selectedDatabase: null,
      selectedSchema: null,
      selectedTable: null,
      schemas: [],
      tables: [],
      loadingSchemas: false,
      loadingTables: false
    }
  },
  methods: {
    async selectDatabase(db) {
      this.selectedDatabase = db
      this.selectedSchema = null
      this.schemas = []
      this.tables = []
      await this.fetchSchemas(db)
    },
    async selectSchema(schema) {
      this.selectedSchema = schema
      this.tables = []
      await this.fetchTables(this.selectedDatabase, schema)
    },
    async fetchSchemas(database) {
      this.loadingSchemas = true
      try {
        const response = await axios.get(`/api/databases/${database}/schemas`)
        this.schemas = response.data.schemas
      } catch (error) {
        console.error('Failed to fetch schemas:', error)
        this.schemas = []
      } finally {
        this.loadingSchemas = false
      }
    },
    async fetchTables(database, schema) {
      this.loadingTables = true
      try {
        const response = await axios.get(`/api/databases/${database}/schemas/${schema}/tables`)
        this.tables = response.data.tables
      } catch (error) {
        console.error('Failed to fetch tables:', error)
        this.tables = []
      } finally {
        this.loadingTables = false
      }
    },
    clearSelection() {
      this.selectedDatabase = null
      this.selectedSchema = null
      this.schemas = []
      this.tables = []
    },
    showTableDetails(table) {
      this.selectedTable = table
    },
    formatDate(isoString) {
      if (!isoString) return 'N/A'
      return new Date(isoString).toLocaleDateString()
    },
    formatDateTime(isoString) {
      if (!isoString) return 'N/A'
      return new Date(isoString).toLocaleString()
    }
  }
}
</script>

<style scoped>
.database-explorer {
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

.explorer-content {
  display: grid;
  grid-template-columns: 250px 1fr;
  gap: 1.5rem;
  min-height: 500px;
}

.sidebar {
  background: var(--card-bg);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.sidebar-header {
  padding: 1rem 1.25rem;
  background: var(--bg-color);
  border-bottom: 1px solid var(--border-color);
}

.sidebar-header h3 {
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--text-color);
}

.database-list {
  padding: 0.5rem;
}

.database-item {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.75rem 1rem;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.database-item:hover {
  background: var(--bg-color);
}

.database-item.selected {
  background: #e0f2fe;
  color: var(--primary-color);
}

.db-icon {
  font-size: 1.1rem;
}

.db-name {
  flex: 1;
  font-size: 0.9rem;
  font-weight: 500;
}

.expand-icon {
  font-size: 0.75rem;
  color: var(--primary-color);
}

.main-content {
  background: var(--card-bg);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  padding: 1.5rem;
}

.placeholder {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--text-muted);
}

.placeholder-icon {
  font-size: 4rem;
  margin-bottom: 1rem;
}

.database-details {
  height: 100%;
}

.breadcrumb {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 1.5rem;
  font-size: 0.9rem;
}

.breadcrumb-item {
  color: var(--primary-color);
  cursor: pointer;
}

.breadcrumb-item.current {
  color: var(--text-color);
  font-weight: 600;
  cursor: default;
}

.breadcrumb-separator {
  color: var(--text-muted);
}

.loading {
  text-align: center;
  padding: 2rem;
  color: var(--text-muted);
}

.empty-state-small {
  text-align: center;
  padding: 2rem;
  color: var(--text-muted);
  font-size: 0.9rem;
}

.schemas-section h3,
.tables-section h3 {
  font-size: 1rem;
  font-weight: 600;
  margin-bottom: 1rem;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
}

.back-button {
  padding: 0.5rem 1rem;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: var(--card-bg);
  cursor: pointer;
  font-size: 0.875rem;
}

.back-button:hover {
  background: var(--bg-color);
}

.schema-grid,
.table-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 1rem;
}

.schema-card,
.table-card {
  background: var(--bg-color);
  padding: 1rem;
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.2s ease;
  border: 1px solid transparent;
}

.schema-card:hover,
.table-card:hover {
  border-color: var(--primary-color);
  box-shadow: 0 4px 12px rgba(41, 181, 232, 0.15);
}

.schema-card {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.schema-icon {
  font-size: 1.5rem;
}

.schema-name {
  font-weight: 500;
}

.table-header {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  margin-bottom: 0.5rem;
}

.table-icon {
  font-size: 1.3rem;
}

.table-name {
  font-weight: 600;
  font-size: 0.95rem;
}

.table-meta {
  display: flex;
  justify-content: space-between;
  font-size: 0.8rem;
  color: var(--text-muted);
}

.table-type {
  background: var(--card-bg);
  padding: 0.15rem 0.5rem;
  border-radius: 4px;
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
  max-width: 600px;
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
  min-width: 100px;
}

.monospace {
  font-family: 'Fira Code', 'Consolas', monospace;
  font-size: 0.9rem;
}

.columns-section {
  margin-top: 1.5rem;
}

.columns-section h4 {
  font-size: 1rem;
  font-weight: 600;
  margin-bottom: 1rem;
}

.columns-table {
  width: 100%;
  border-collapse: collapse;
}

.columns-table th,
.columns-table td {
  text-align: left;
  padding: 0.75rem;
  border-bottom: 1px solid var(--border-color);
}

.columns-table th {
  background: var(--bg-color);
  font-weight: 600;
  font-size: 0.9rem;
}

.columns-table td {
  font-size: 0.9rem;
}
</style>
