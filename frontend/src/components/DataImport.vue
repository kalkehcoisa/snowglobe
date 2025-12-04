<template>
  <div class="data-import">
    <div class="import-header">
      <h2>📥 Import Data</h2>
      <p class="subtitle">Import SQL, CSV, JSON, Parquet, or Jupyter notebook files</p>
    </div>

    <div class="import-content">
      <!-- File Upload Zone -->
      <div 
        class="upload-zone"
        :class="{ 'drag-over': isDragOver }"
        @dragover.prevent="isDragOver = true"
        @dragleave="isDragOver = false"
        @drop.prevent="handleDrop"
        @click="triggerFileInput"
      >
        <input 
          type="file" 
          ref="fileInput" 
          @change="handleFileSelect"
          :accept="acceptedTypes"
          hidden
        />
        <div class="upload-icon">📁</div>
        <p class="upload-text">
          Drag & drop files here or <span class="link">click to browse</span>
        </p>
        <p class="upload-hint">
          Supported: .sql, .csv, .json, .parquet, .ipynb, .tsv, .txt
        </p>
      </div>

      <!-- Selected File -->
      <div v-if="selectedFile" class="selected-file">
        <div class="file-info">
          <span class="file-icon">{{ getFileIcon(selectedFile.name) }}</span>
          <div class="file-details">
            <span class="file-name">{{ selectedFile.name }}</span>
            <span class="file-size">{{ formatFileSize(selectedFile.size) }}</span>
          </div>
          <button class="remove-btn" @click="clearFile">×</button>
        </div>

        <!-- Import Options -->
        <div class="import-options">
          <h4>Import Options</h4>
          
          <!-- CSV Options -->
          <div v-if="fileType === 'csv'" class="option-group">
            <div class="option-row">
              <label>Table Name:</label>
              <input v-model="options.table_name" type="text" :placeholder="suggestedTableName" />
            </div>
            <div class="option-row">
              <label>Delimiter:</label>
              <select v-model="options.delimiter">
                <option value=",">Comma (,)</option>
                <option value=";">Semicolon (;)</option>
                <option value="\t">Tab</option>
                <option value="|">Pipe (|)</option>
              </select>
            </div>
            <div class="option-row">
              <label>
                <input type="checkbox" v-model="options.has_header" />
                First row is header
              </label>
            </div>
            <div class="option-row">
              <label>
                <input type="checkbox" v-model="options.create_table" />
                Create table if not exists
              </label>
            </div>
            <div class="option-row">
              <label>
                <input type="checkbox" v-model="options.truncate" />
                Truncate existing data
              </label>
            </div>
          </div>

          <!-- SQL Options -->
          <div v-if="fileType === 'sql'" class="option-group">
            <div class="option-row">
              <label>
                <input type="checkbox" v-model="options.stop_on_error" />
                Stop on first error
              </label>
            </div>
          </div>

          <!-- JSON Options -->
          <div v-if="fileType === 'json'" class="option-group">
            <div class="option-row">
              <label>Table Name:</label>
              <input v-model="options.table_name" type="text" :placeholder="suggestedTableName" />
            </div>
            <div class="option-row">
              <label>JSON Path (optional):</label>
              <input v-model="options.json_path" type="text" placeholder="e.g., data.items" />
            </div>
            <div class="option-row">
              <label>
                <input type="checkbox" v-model="options.flatten" />
                Flatten nested objects
              </label>
            </div>
          </div>

          <!-- Notebook Options -->
          <div v-if="fileType === 'notebook'" class="option-group">
            <div class="option-row">
              <label>
                <input type="checkbox" v-model="options.extract_only" />
                Extract SQL only (don't execute)
              </label>
            </div>
          </div>
        </div>

        <!-- Import Button -->
        <div class="import-actions">
          <button class="btn-secondary" @click="previewFile" :disabled="isLoading">
            👀 Preview
          </button>
          <button class="btn-primary" @click="importFile" :disabled="isLoading">
            {{ isLoading ? '⏳ Importing...' : '📥 Import' }}
          </button>
        </div>
      </div>

      <!-- Preview -->
      <div v-if="preview" class="preview-section">
        <h4>Preview</h4>
        <div v-if="preview.type === 'csv'" class="preview-table-container">
          <table class="preview-table">
            <thead>
              <tr>
                <th v-for="col in preview.columns" :key="col">{{ col }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, idx) in preview.rows.slice(0, 5)" :key="idx">
                <td v-for="(val, cidx) in row" :key="cidx">{{ val }}</td>
              </tr>
            </tbody>
          </table>
          <p class="preview-note" v-if="preview.rows.length > 5">
            Showing 5 of {{ preview.rows.length }} rows
          </p>
        </div>
        <div v-if="preview.type === 'sql'" class="preview-sql">
          <p>{{ preview.statement_count }} SQL statement(s) found</p>
          <pre class="sql-preview">{{ preview.statements[0] }}</pre>
          <p v-if="preview.statements.length > 1" class="preview-note">
            ... and {{ preview.statements.length - 1 }} more
          </p>
        </div>
      </div>

      <!-- Results -->
      <div v-if="importResult" class="result-section">
        <div :class="['result-header', importResult.success ? 'success' : 'error']">
          <span class="result-icon">{{ importResult.success ? '✅' : '❌' }}</span>
          <span class="result-title">
            {{ importResult.success ? 'Import Successful' : 'Import Failed' }}
          </span>
        </div>
        <div class="result-details">
          <div v-if="importResult.file_type === 'csv' || importResult.file_type === 'json'">
            <p>Table: <code>{{ importResult.table_name }}</code></p>
            <p>Rows imported: {{ importResult.rows_inserted }} / {{ importResult.rows_total }}</p>
            <p v-if="importResult.columns">Columns: {{ importResult.columns.join(', ') }}</p>
          </div>
          <div v-if="importResult.file_type === 'sql'">
            <p>Statements executed: {{ importResult.statements_success }} / {{ importResult.statements_total }}</p>
            <p v-if="importResult.statements_failed > 0" class="error-text">
              {{ importResult.statements_failed }} statement(s) failed
            </p>
          </div>
          <div v-if="importResult.error" class="error-message">
            {{ importResult.error }}
          </div>
          <div v-if="importResult.errors && importResult.errors.length > 0" class="error-list">
            <p>Errors:</p>
            <ul>
              <li v-for="(err, idx) in importResult.errors.slice(0, 5)" :key="idx">
                Row {{ err.row }}: {{ err.error }}
              </li>
            </ul>
          </div>
        </div>
      </div>

      <!-- Recent Imports -->
      <div v-if="recentImports.length > 0" class="recent-section">
        <h4>Recent Imports</h4>
        <div class="recent-list">
          <div 
            v-for="imp in recentImports" 
            :key="imp.imported_at"
            class="recent-item"
          >
            <span class="recent-icon">{{ getFileIcon(imp.file) }}</span>
            <div class="recent-info">
              <span class="recent-name">{{ getFileName(imp.file) }}</span>
              <span class="recent-time">{{ formatTime(imp.imported_at) }}</span>
            </div>
            <span :class="['recent-status', imp.success ? 'success' : 'error']">
              {{ imp.success ? '✓' : '✗' }}
            </span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'DataImport',
  emits: ['import-complete'],
  data() {
    return {
      selectedFile: null,
      isDragOver: false,
      isLoading: false,
      
      options: {
        table_name: '',
        delimiter: ',',
        has_header: true,
        create_table: true,
        truncate: false,
        stop_on_error: false,
        json_path: '',
        flatten: false,
        extract_only: false
      },
      
      preview: null,
      importResult: null,
      recentImports: []
    }
  },
  computed: {
    acceptedTypes() {
      return '.sql,.csv,.json,.parquet,.ipynb,.tsv,.txt'
    },
    fileType() {
      if (!this.selectedFile) return null
      const ext = this.selectedFile.name.split('.').pop().toLowerCase()
      if (ext === 'csv' || ext === 'tsv') return 'csv'
      if (ext === 'sql' || ext === 'txt') return 'sql'
      if (ext === 'json') return 'json'
      if (ext === 'parquet') return 'parquet'
      if (ext === 'ipynb') return 'notebook'
      return null
    },
    suggestedTableName() {
      if (!this.selectedFile) return 'TABLE_NAME'
      return this.selectedFile.name
        .replace(/\.[^.]+$/, '')  // Remove extension
        .replace(/[^a-zA-Z0-9_]/g, '_')  // Replace invalid chars
        .toUpperCase()
    }
  },
  methods: {
    triggerFileInput() {
      this.$refs.fileInput.click()
    },
    
    handleFileSelect(event) {
      const file = event.target.files[0]
      if (file) {
        this.selectFile(file)
      }
    },
    
    handleDrop(event) {
      this.isDragOver = false
      const file = event.dataTransfer.files[0]
      if (file) {
        this.selectFile(file)
      }
    },
    
    selectFile(file) {
      this.selectedFile = file
      this.preview = null
      this.importResult = null
      this.options.table_name = ''
      
      // Auto-set delimiter for TSV files
      if (file.name.endsWith('.tsv')) {
        this.options.delimiter = '\t'
      } else {
        this.options.delimiter = ','
      }
    },
    
    clearFile() {
      this.selectedFile = null
      this.preview = null
      this.importResult = null
      this.$refs.fileInput.value = ''
    },
    
    async previewFile() {
      if (!this.selectedFile) return
      
      try {
        if (this.fileType === 'csv') {
          const text = await this.selectedFile.text()
          const lines = text.split('\n').filter(l => l.trim())
          const delimiter = this.options.delimiter === '\\t' ? '\t' : this.options.delimiter
          
          const rows = lines.map(line => {
            // Simple CSV parsing (doesn't handle quoted fields perfectly)
            return line.split(delimiter)
          })
          
          const columns = this.options.has_header ? rows[0] : rows[0].map((_, i) => `COL_${i}`)
          const dataRows = this.options.has_header ? rows.slice(1) : rows
          
          this.preview = {
            type: 'csv',
            columns,
            rows: dataRows
          }
        } else if (this.fileType === 'sql') {
          const text = await this.selectedFile.text()
          const statements = text.split(';').filter(s => s.trim())
          
          this.preview = {
            type: 'sql',
            statement_count: statements.length,
            statements: statements.map(s => s.trim().substring(0, 200))
          }
        }
      } catch (error) {
        console.error('Preview failed:', error)
      }
    },
    
    async importFile() {
      if (!this.selectedFile || this.isLoading) return
      
      this.isLoading = true
      this.importResult = null
      
      try {
        const formData = new FormData()
        formData.append('file', this.selectedFile)
        
        // Add options
        if (this.options.table_name) {
          formData.append('table_name', this.options.table_name)
        }
        if (this.fileType === 'csv') {
          formData.append('delimiter', this.options.delimiter)
          formData.append('has_header', this.options.has_header)
          formData.append('create_table', this.options.create_table)
          formData.append('truncate', this.options.truncate)
        }
        
        const response = await axios.post('/api/import/file', formData, {
          headers: {
            'Content-Type': 'multipart/form-data'
          }
        })
        
        this.importResult = response.data
        this.recentImports.unshift(response.data)
        
        // Keep only last 10 imports
        if (this.recentImports.length > 10) {
          this.recentImports = this.recentImports.slice(0, 10)
        }
        
        if (response.data.success) {
          this.$emit('import-complete', response.data)
        }
        
      } catch (error) {
        console.error('Import failed:', error)
        this.importResult = {
          success: false,
          error: error.response?.data?.error || error.message
        }
      } finally {
        this.isLoading = false
      }
    },
    
    getFileIcon(filename) {
      if (!filename) return '📄'
      const ext = filename.split('.').pop().toLowerCase()
      const icons = {
        'sql': '📜',
        'csv': '📊',
        'tsv': '📊',
        'json': '📋',
        'parquet': '🗃️',
        'ipynb': '📓',
        'txt': '📝'
      }
      return icons[ext] || '📄'
    },
    
    formatFileSize(bytes) {
      if (bytes < 1024) return bytes + ' B'
      if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB'
      return (bytes / (1024 * 1024)).toFixed(1) + ' MB'
    },
    
    getFileName(filepath) {
      return filepath.split('/').pop().split('\\').pop()
    },
    
    formatTime(isoString) {
      if (!isoString) return ''
      return new Date(isoString).toLocaleTimeString()
    }
  }
}
</script>

<style scoped>
.data-import {
  max-width: 800px;
  margin: 0 auto;
}

.import-header {
  margin-bottom: 1.5rem;
}

.import-header h2 {
  font-size: 1.5rem;
  margin: 0 0 0.5rem 0;
}

.subtitle {
  color: var(--text-muted);
  margin: 0;
}

.upload-zone {
  border: 2px dashed var(--border-color);
  border-radius: 12px;
  padding: 3rem;
  text-align: center;
  cursor: pointer;
  transition: all 0.2s;
  background: var(--bg-color);
}

.upload-zone:hover,
.upload-zone.drag-over {
  border-color: var(--primary-color);
  background: rgba(41, 181, 232, 0.05);
}

.upload-icon {
  font-size: 3rem;
  margin-bottom: 1rem;
}

.upload-text {
  font-size: 1.1rem;
  margin: 0 0 0.5rem 0;
}

.upload-text .link {
  color: var(--primary-color);
  text-decoration: underline;
}

.upload-hint {
  font-size: 0.85rem;
  color: var(--text-muted);
  margin: 0;
}

.selected-file {
  margin-top: 1.5rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  padding: 1.5rem;
}

.file-info {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding-bottom: 1rem;
  border-bottom: 1px solid var(--border-color);
}

.file-icon {
  font-size: 2rem;
}

.file-details {
  flex: 1;
}

.file-name {
  font-weight: 600;
  display: block;
}

.file-size {
  font-size: 0.85rem;
  color: var(--text-muted);
}

.remove-btn {
  background: none;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  color: var(--text-muted);
}

.import-options {
  padding: 1rem 0;
}

.import-options h4 {
  margin: 0 0 1rem 0;
  font-size: 0.95rem;
}

.option-group {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.option-row {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.option-row label {
  min-width: 120px;
  font-size: 0.9rem;
}

.option-row input[type="text"],
.option-row select {
  flex: 1;
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  font-size: 0.9rem;
}

.option-row input[type="checkbox"] {
  margin-right: 0.5rem;
}

.import-actions {
  display: flex;
  gap: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--border-color);
  margin-top: 1rem;
}

.btn-primary,
.btn-secondary {
  flex: 1;
  padding: 0.75rem 1.5rem;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-size: 0.95rem;
  font-weight: 500;
}

.btn-primary {
  background: var(--primary-color);
  color: white;
}

.btn-primary:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.btn-secondary {
  background: var(--bg-color);
  border: 1px solid var(--border-color);
}

.preview-section,
.result-section {
  margin-top: 1.5rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 12px;
  padding: 1.5rem;
}

.preview-section h4,
.result-section h4 {
  margin: 0 0 1rem 0;
}

.preview-table-container {
  overflow-x: auto;
}

.preview-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}

.preview-table th,
.preview-table td {
  padding: 0.5rem;
  text-align: left;
  border-bottom: 1px solid var(--border-color);
}

.preview-table th {
  background: var(--bg-color);
  font-weight: 500;
}

.preview-note {
  font-size: 0.85rem;
  color: var(--text-muted);
  margin-top: 0.5rem;
}

.sql-preview {
  background: #1e1e1e;
  color: #d4d4d4;
  padding: 1rem;
  border-radius: 6px;
  font-family: monospace;
  font-size: 0.85rem;
  overflow-x: auto;
  white-space: pre-wrap;
}

.result-header {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding-bottom: 1rem;
  border-bottom: 1px solid var(--border-color);
  margin-bottom: 1rem;
}

.result-header.success {
  color: #10b981;
}

.result-header.error {
  color: #ef4444;
}

.result-icon {
  font-size: 1.5rem;
}

.result-title {
  font-weight: 600;
  font-size: 1.1rem;
}

.result-details p {
  margin: 0.5rem 0;
}

.result-details code {
  background: var(--bg-color);
  padding: 0.2rem 0.4rem;
  border-radius: 4px;
  font-family: monospace;
}

.error-text {
  color: #ef4444;
}

.error-message {
  background: #fee;
  color: #c33;
  padding: 1rem;
  border-radius: 6px;
  margin-top: 1rem;
}

.error-list {
  margin-top: 1rem;
}

.error-list ul {
  margin: 0.5rem 0 0 1.5rem;
  padding: 0;
}

.error-list li {
  font-size: 0.85rem;
  color: #ef4444;
}

.recent-section {
  margin-top: 1.5rem;
}

.recent-section h4 {
  margin: 0 0 1rem 0;
}

.recent-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.recent-item {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.75rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
}

.recent-icon {
  font-size: 1.25rem;
}

.recent-info {
  flex: 1;
}

.recent-name {
  display: block;
  font-weight: 500;
  font-size: 0.9rem;
}

.recent-time {
  font-size: 0.8rem;
  color: var(--text-muted);
}

.recent-status.success {
  color: #10b981;
}

.recent-status.error {
  color: #ef4444;
}
</style>
