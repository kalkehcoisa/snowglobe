<template>
  <div class="data-export">
    <div class="panel-header">
      <h2 class="section-title">📤 Data Export</h2>
    </div>

    <!-- Export Type Selection -->
    <div class="export-type-selector">
      <div 
        v-for="type in exportTypes" 
        :key="type.id"
        :class="['type-card', { active: selectedType === type.id }]"
        @click="selectedType = type.id"
      >
        <span class="type-icon">{{ type.icon }}</span>
        <span class="type-name">{{ type.name }}</span>
        <span class="type-description">{{ type.description }}</span>
      </div>
    </div>

    <!-- Export Configuration -->
    <div class="export-config" v-if="selectedType">
      <!-- Query Export -->
      <div v-if="selectedType === 'query'" class="config-section">
        <h3>Export Query Results</h3>
        <div class="form-group">
          <label>SQL Query</label>
          <textarea 
            v-model="queryConfig.sql" 
            class="sql-input"
            placeholder="SELECT * FROM my_table LIMIT 1000"
          ></textarea>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Format</label>
            <select v-model="queryConfig.format">
              <option v-for="fmt in formats" :key="fmt" :value="fmt">{{ fmt.toUpperCase() }}</option>
            </select>
          </div>
        </div>
      </div>

      <!-- Table Export -->
      <div v-if="selectedType === 'table'" class="config-section">
        <h3>Export Table</h3>
        <div class="form-row">
          <div class="form-group">
            <label>Database</label>
            <select v-model="tableConfig.database" @change="onDatabaseChange">
              <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
            </select>
          </div>
          <div class="form-group">
            <label>Schema</label>
            <select v-model="tableConfig.schema" @change="onSchemaChange">
              <option v-for="sch in schemas" :key="sch.name" :value="sch.name">{{ sch.name }}</option>
            </select>
          </div>
          <div class="form-group">
            <label>Table</label>
            <select v-model="tableConfig.table">
              <option v-for="tbl in tables" :key="tbl.name" :value="tbl.name">{{ tbl.name }}</option>
            </select>
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Format</label>
            <select v-model="tableConfig.format">
              <option v-for="fmt in formats" :key="fmt" :value="fmt">{{ fmt.toUpperCase() }}</option>
            </select>
          </div>
          <div class="form-group checkbox-group">
            <label>
              <input type="checkbox" v-model="tableConfig.includeDDL">
              Include DDL (CREATE TABLE)
            </label>
          </div>
        </div>
      </div>

      <!-- Schema Export -->
      <div v-if="selectedType === 'schema'" class="config-section">
        <h3>Export Schema</h3>
        <div class="form-row">
          <div class="form-group">
            <label>Database</label>
            <select v-model="schemaConfig.database" @change="onDatabaseChange">
              <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
            </select>
          </div>
          <div class="form-group">
            <label>Schema</label>
            <select v-model="schemaConfig.schema">
              <option v-for="sch in schemas" :key="sch.name" :value="sch.name">{{ sch.name }}</option>
            </select>
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Format</label>
            <select v-model="schemaConfig.format">
              <option v-for="fmt in formats" :key="fmt" :value="fmt">{{ fmt.toUpperCase() }}</option>
            </select>
          </div>
          <div class="form-group checkbox-group">
            <label>
              <input type="checkbox" v-model="schemaConfig.includeData">
              Include Data
            </label>
          </div>
          <div class="form-group checkbox-group">
            <label>
              <input type="checkbox" v-model="schemaConfig.includeViews">
              Include Views
            </label>
          </div>
        </div>
      </div>

      <!-- Database Export -->
      <div v-if="selectedType === 'database'" class="config-section">
        <h3>Export Database</h3>
        <div class="form-row">
          <div class="form-group">
            <label>Database</label>
            <select v-model="databaseConfig.database">
              <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
            </select>
          </div>
          <div class="form-group">
            <label>Format</label>
            <select v-model="databaseConfig.format">
              <option v-for="fmt in formats" :key="fmt" :value="fmt">{{ fmt.toUpperCase() }}</option>
            </select>
          </div>
        </div>
        <div class="form-row">
          <div class="form-group checkbox-group">
            <label>
              <input type="checkbox" v-model="databaseConfig.includeData">
              Include Data
            </label>
          </div>
        </div>
      </div>

      <!-- DDL Export -->
      <div v-if="selectedType === 'ddl'" class="config-section">
        <h3>Export DDL Only</h3>
        <div class="form-row">
          <div class="form-group">
            <label>Database (optional)</label>
            <select v-model="ddlConfig.database" @change="onDatabaseChange">
              <option value="">All Databases</option>
              <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
            </select>
          </div>
          <div class="form-group">
            <label>Schema (optional)</label>
            <select v-model="ddlConfig.schema" :disabled="!ddlConfig.database">
              <option value="">All Schemas</option>
              <option v-for="sch in schemas" :key="sch.name" :value="sch.name">{{ sch.name }}</option>
            </select>
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Object Type</label>
            <select v-model="ddlConfig.objectType">
              <option value="">All Objects</option>
              <option value="TABLE">Tables Only</option>
              <option value="VIEW">Views Only</option>
            </select>
          </div>
        </div>
      </div>

      <!-- Multiple Tables Export -->
      <div v-if="selectedType === 'tables'" class="config-section">
        <h3>Export Multiple Tables</h3>
        <div class="form-row">
          <div class="form-group">
            <label>Database</label>
            <select v-model="multiTableConfig.database" @change="onDatabaseChange">
              <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
            </select>
          </div>
          <div class="form-group">
            <label>Schema</label>
            <select v-model="multiTableConfig.schema" @change="onSchemaChange">
              <option v-for="sch in schemas" :key="sch.name" :value="sch.name">{{ sch.name }}</option>
            </select>
          </div>
          <div class="form-group">
            <label>Format</label>
            <select v-model="multiTableConfig.format">
              <option v-for="fmt in formats" :key="fmt" :value="fmt">{{ fmt.toUpperCase() }}</option>
            </select>
          </div>
        </div>
        <div class="tables-selection">
          <h4>Select Tables</h4>
          <div class="select-all">
            <label>
              <input type="checkbox" @change="toggleAllTables" :checked="allTablesSelected">
              Select All
            </label>
          </div>
          <div class="tables-list">
            <label v-for="tbl in tables" :key="tbl.name" class="table-checkbox">
              <input type="checkbox" v-model="multiTableConfig.selectedTables" :value="tbl.name">
              {{ tbl.name }}
            </label>
          </div>
        </div>
      </div>

      <!-- Format Options -->
      <div v-if="showFormatOptions" class="format-options">
        <h4>Format Options</h4>
        <!-- CSV Options -->
        <div v-if="currentFormat === 'csv'" class="options-grid">
          <div class="form-group">
            <label>Delimiter</label>
            <select v-model="formatOptions.delimiter">
              <option value=",">,</option>
              <option value=";">;</option>
              <option value="\t">Tab</option>
              <option value="|">|</option>
            </select>
          </div>
          <div class="form-group checkbox-group">
            <label>
              <input type="checkbox" v-model="formatOptions.includeHeader">
              Include Header
            </label>
          </div>
        </div>
        <!-- JSON Options -->
        <div v-if="currentFormat === 'json'" class="options-grid">
          <div class="form-group">
            <label>Orientation</label>
            <select v-model="formatOptions.orient">
              <option value="records">Records (array of objects)</option>
              <option value="columns">Columns (object of arrays)</option>
              <option value="values">Values (columns + data)</option>
            </select>
          </div>
          <div class="form-group">
            <label>Indent</label>
            <select v-model="formatOptions.indent">
              <option :value="0">None</option>
              <option :value="2">2 spaces</option>
              <option :value="4">4 spaces</option>
            </select>
          </div>
        </div>
        <!-- SQL Options -->
        <div v-if="currentFormat === 'sql'" class="options-grid">
          <div class="form-group checkbox-group">
            <label>
              <input type="checkbox" v-model="formatOptions.includeCreate">
              Include CREATE TABLE
            </label>
          </div>
          <div class="form-group">
            <label>Batch Size</label>
            <input type="number" v-model.number="formatOptions.batchSize" min="100" max="10000">
          </div>
        </div>
      </div>

      <!-- Export Button -->
      <div class="export-actions">
        <button 
          class="btn-primary export-btn" 
          @click="executeExport" 
          :disabled="isExporting || !canExport"
        >
          <span v-if="isExporting">⏳ Exporting...</span>
          <span v-else>📤 Export</span>
        </button>
      </div>
    </div>

    <!-- Export Result -->
    <div v-if="exportResult" class="export-result">
      <div :class="['result-card', exportResult.success ? 'success' : 'error']">
        <div class="result-header">
          <span v-if="exportResult.success">✅ Export Successful</span>
          <span v-else>❌ Export Failed</span>
        </div>
        <div class="result-body">
          <div v-if="exportResult.success">
            <p><strong>Format:</strong> {{ exportResult.format?.toUpperCase() }}</p>
            <p v-if="exportResult.row_count"><strong>Rows:</strong> {{ exportResult.row_count }}</p>
            <p v-if="exportResult.table_count"><strong>Tables:</strong> {{ exportResult.table_count }}</p>
            <p><strong>Filename:</strong> {{ exportResult.filename }}</p>
            
            <div class="result-actions">
              <button class="btn-secondary" @click="downloadExport" v-if="exportResult.content">
                💾 Download
              </button>
              <button class="btn-secondary" @click="copyToClipboard" v-if="!exportResult.binary">
                📋 Copy to Clipboard
              </button>
            </div>
            
            <!-- Preview for text formats -->
            <div v-if="!exportResult.binary && exportResult.content" class="content-preview">
              <h4>Preview (first 2000 chars)</h4>
              <pre>{{ exportResult.content.substring(0, 2000) }}{{ exportResult.content.length > 2000 ? '...' : '' }}</pre>
            </div>
          </div>
          <div v-else>
            <p class="error-message">{{ exportResult.error }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'DataExport',
  data() {
    return {
      selectedType: null,
      exportTypes: [
        { id: 'query', name: 'Query', icon: '🔍', description: 'Export query results' },
        { id: 'table', name: 'Table', icon: '📊', description: 'Export a single table' },
        { id: 'schema', name: 'Schema', icon: '📁', description: 'Export entire schema' },
        { id: 'database', name: 'Database', icon: '🗄️', description: 'Export entire database' },
        { id: 'ddl', name: 'DDL Only', icon: '📝', description: 'Export structure only' },
        { id: 'tables', name: 'Multiple', icon: '📋', description: 'Select multiple tables' }
      ],
      formats: ['csv', 'json', 'jsonl', 'sql', 'parquet', 'excel'],
      databases: [],
      schemas: [],
      tables: [],
      
      // Config for each export type
      queryConfig: {
        sql: '',
        format: 'csv'
      },
      tableConfig: {
        database: '',
        schema: '',
        table: '',
        format: 'csv',
        includeDDL: false
      },
      schemaConfig: {
        database: '',
        schema: '',
        format: 'sql',
        includeData: true,
        includeViews: true
      },
      databaseConfig: {
        database: '',
        format: 'sql',
        includeData: true
      },
      ddlConfig: {
        database: '',
        schema: '',
        objectType: ''
      },
      multiTableConfig: {
        database: '',
        schema: '',
        format: 'csv',
        selectedTables: []
      },
      
      // Format options
      formatOptions: {
        delimiter: ',',
        includeHeader: true,
        orient: 'records',
        indent: 2,
        includeCreate: true,
        batchSize: 1000
      },
      
      isExporting: false,
      exportResult: null
    }
  },
  computed: {
    currentFormat() {
      switch (this.selectedType) {
        case 'query': return this.queryConfig.format
        case 'table': return this.tableConfig.format
        case 'schema': return this.schemaConfig.format
        case 'database': return this.databaseConfig.format
        case 'tables': return this.multiTableConfig.format
        case 'ddl': return 'sql'
        default: return 'csv'
      }
    },
    showFormatOptions() {
      return ['csv', 'json', 'sql'].includes(this.currentFormat)
    },
    canExport() {
      switch (this.selectedType) {
        case 'query':
          return this.queryConfig.sql.trim().length > 0
        case 'table':
          return this.tableConfig.database && this.tableConfig.schema && this.tableConfig.table
        case 'schema':
          return this.schemaConfig.database && this.schemaConfig.schema
        case 'database':
          return this.databaseConfig.database
        case 'ddl':
          return true
        case 'tables':
          return this.multiTableConfig.selectedTables.length > 0
        default:
          return false
      }
    },
    allTablesSelected() {
      return this.tables.length > 0 && 
             this.multiTableConfig.selectedTables.length === this.tables.length
    }
  },
  methods: {
    async fetchDatabases() {
      try {
        const response = await axios.get('/api/databases')
        this.databases = response.data.databases
      } catch (error) {
        console.error('Failed to fetch databases:', error)
      }
    },
    
    async onDatabaseChange() {
      const database = this.tableConfig.database || 
                       this.schemaConfig.database || 
                       this.databaseConfig.database ||
                       this.ddlConfig.database ||
                       this.multiTableConfig.database
      
      if (!database) {
        this.schemas = []
        this.tables = []
        return
      }
      
      try {
        const response = await axios.get(`/api/databases/${database}/schemas`)
        this.schemas = response.data.schemas
      } catch (error) {
        console.error('Failed to fetch schemas:', error)
        this.schemas = []
      }
    },
    
    async onSchemaChange() {
      const database = this.tableConfig.database || this.multiTableConfig.database
      const schema = this.tableConfig.schema || this.multiTableConfig.schema
      
      if (!database || !schema) {
        this.tables = []
        return
      }
      
      try {
        const response = await axios.get(`/api/databases/${database}/schemas/${schema}/tables`)
        this.tables = response.data.tables
      } catch (error) {
        console.error('Failed to fetch tables:', error)
        this.tables = []
      }
    },
    
    toggleAllTables(event) {
      if (event.target.checked) {
        this.multiTableConfig.selectedTables = this.tables.map(t => t.name)
      } else {
        this.multiTableConfig.selectedTables = []
      }
    },
    
    async executeExport() {
      this.isExporting = true
      this.exportResult = null
      
      try {
        let response
        const options = { ...this.formatOptions }
        
        switch (this.selectedType) {
          case 'query':
            response = await axios.post('/api/export/query', {
              sql: this.queryConfig.sql,
              format: this.queryConfig.format,
              options
            })
            break
            
          case 'table':
            response = await axios.post('/api/export/table', {
              database: this.tableConfig.database,
              schema_name: this.tableConfig.schema,
              table: this.tableConfig.table,
              format: this.tableConfig.format,
              options: {
                ...options,
                include_ddl: this.tableConfig.includeDDL
              }
            })
            break
            
          case 'schema':
            response = await axios.post('/api/export/schema', {
              database: this.schemaConfig.database,
              schema_name: this.schemaConfig.schema,
              format: this.schemaConfig.format,
              options: {
                ...options,
                include_data: this.schemaConfig.includeData,
                include_views: this.schemaConfig.includeViews
              }
            })
            break
            
          case 'database':
            response = await axios.post('/api/export/database', {
              database: this.databaseConfig.database,
              format: this.databaseConfig.format,
              options: {
                ...options,
                include_data: this.databaseConfig.includeData
              }
            })
            break
            
          case 'ddl':
            response = await axios.get('/api/export/ddl', {
              params: {
                database: this.ddlConfig.database || undefined,
                schema: this.ddlConfig.schema || undefined,
                object_type: this.ddlConfig.objectType || undefined
              }
            })
            break
            
          case 'tables':
            const tables = this.multiTableConfig.selectedTables.map(t => ({
              database: this.multiTableConfig.database,
              schema: this.multiTableConfig.schema,
              table: t
            }))
            response = await axios.post('/api/export/tables', {
              tables,
              format: this.multiTableConfig.format,
              options
            })
            break
        }
        
        this.exportResult = response.data
      } catch (error) {
        this.exportResult = {
          success: false,
          error: error.response?.data?.error || error.message
        }
      } finally {
        this.isExporting = false
      }
    },
    
    downloadExport() {
      if (!this.exportResult?.content) return
      
      let content = this.exportResult.content
      let type = this.exportResult.content_type || 'text/plain'
      
      // Handle binary content
      if (this.exportResult.binary) {
        const byteCharacters = atob(content)
        const byteNumbers = new Array(byteCharacters.length)
        for (let i = 0; i < byteCharacters.length; i++) {
          byteNumbers[i] = byteCharacters.charCodeAt(i)
        }
        const byteArray = new Uint8Array(byteNumbers)
        content = byteArray
      }
      
      const blob = new Blob([content], { type })
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = this.exportResult.filename || 'export'
      a.click()
      window.URL.revokeObjectURL(url)
    },
    
    async copyToClipboard() {
      if (!this.exportResult?.content) return
      
      try {
        await navigator.clipboard.writeText(this.exportResult.content)
        alert('Copied to clipboard!')
      } catch (error) {
        console.error('Failed to copy:', error)
      }
    }
  },
  async mounted() {
    await this.fetchDatabases()
  }
}
</script>

<style scoped>
.data-export {
  padding: 1.5rem;
}

.panel-header {
  margin-bottom: 1.5rem;
}

.section-title {
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--text-color);
}

.export-type-selector {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.type-card {
  background: var(--card-bg);
  border: 2px solid var(--border-color);
  border-radius: 12px;
  padding: 1.25rem;
  cursor: pointer;
  transition: all 0.2s ease;
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
}

.type-card:hover {
  border-color: var(--primary-color);
  transform: translateY(-2px);
}

.type-card.active {
  border-color: var(--primary-color);
  background: #e0f2fe;
}

.type-icon {
  font-size: 2rem;
  margin-bottom: 0.5rem;
}

.type-name {
  font-weight: 600;
  margin-bottom: 0.25rem;
}

.type-description {
  font-size: 0.8rem;
  color: var(--text-muted);
}

.export-config {
  background: var(--card-bg);
  border-radius: 12px;
  padding: 1.5rem;
  border: 1px solid var(--border-color);
}

.config-section h3 {
  font-size: 1.1rem;
  font-weight: 600;
  margin-bottom: 1rem;
}

.form-row {
  display: flex;
  gap: 1rem;
  margin-bottom: 1rem;
  flex-wrap: wrap;
}

.form-group {
  flex: 1;
  min-width: 150px;
}

.form-group label {
  display: block;
  font-size: 0.9rem;
  font-weight: 500;
  margin-bottom: 0.5rem;
}

.form-group select,
.form-group input[type="text"],
.form-group input[type="number"] {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  background: var(--bg-color);
  font-size: 0.9rem;
}

.sql-input {
  width: 100%;
  min-height: 120px;
  padding: 0.75rem;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  font-family: monospace;
  font-size: 0.9rem;
  resize: vertical;
}

.checkbox-group {
  display: flex;
  align-items: center;
}

.checkbox-group label {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0;
  cursor: pointer;
}

.tables-selection {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--border-color);
}

.tables-selection h4 {
  margin-bottom: 0.75rem;
}

.select-all {
  margin-bottom: 0.75rem;
  padding-bottom: 0.5rem;
  border-bottom: 1px dashed var(--border-color);
}

.tables-list {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 0.5rem;
  max-height: 200px;
  overflow-y: auto;
}

.table-checkbox {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  cursor: pointer;
  padding: 0.25rem;
  border-radius: 4px;
}

.table-checkbox:hover {
  background: var(--bg-color);
}

.format-options {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--border-color);
}

.format-options h4 {
  margin-bottom: 0.75rem;
}

.options-grid {
  display: flex;
  gap: 1rem;
  flex-wrap: wrap;
}

.export-actions {
  margin-top: 1.5rem;
  display: flex;
  justify-content: flex-end;
}

.btn-primary {
  background: var(--primary-color);
  color: white;
  border: none;
  padding: 0.75rem 1.5rem;
  border-radius: 8px;
  font-size: 1rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-primary:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(41, 181, 232, 0.3);
}

.btn-primary:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.btn-secondary {
  background: var(--card-bg);
  color: var(--text-color);
  border: 1px solid var(--border-color);
  padding: 0.5rem 1rem;
  border-radius: 6px;
  font-size: 0.9rem;
  cursor: pointer;
  margin-right: 0.5rem;
}

.export-result {
  margin-top: 1.5rem;
}

.result-card {
  border-radius: 12px;
  overflow: hidden;
}

.result-card.success {
  background: #f0fdf4;
  border: 1px solid #22c55e;
}

.result-card.error {
  background: #fef2f2;
  border: 1px solid #ef4444;
}

.result-header {
  padding: 1rem 1.5rem;
  font-weight: 600;
}

.result-card.success .result-header {
  background: #dcfce7;
}

.result-card.error .result-header {
  background: #fee2e2;
}

.result-body {
  padding: 1.5rem;
}

.result-body p {
  margin: 0.5rem 0;
}

.error-message {
  color: #dc2626;
}

.result-actions {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--border-color);
}

.content-preview {
  margin-top: 1rem;
}

.content-preview h4 {
  margin-bottom: 0.5rem;
}

.content-preview pre {
  background: #1e1e1e;
  color: #d4d4d4;
  padding: 1rem;
  border-radius: 8px;
  overflow-x: auto;
  max-height: 300px;
  font-size: 0.85rem;
}
</style>
