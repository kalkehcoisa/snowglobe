<template>
  <div class="worksheet-panel">
    <div class="panel-header">
      <h2>📝 SQL Worksheet</h2>
      <div class="header-actions">
        <button class="btn-secondary" @click="clearEditor" title="Clear">
          🗑️ Clear
        </button>
        <button class="btn-secondary" @click="formatSql" title="Format SQL">
          ✨ Format
        </button>
        <button class="btn-primary" @click="executeQuery" :disabled="isExecuting">
          {{ isExecuting ? '⏳ Executing...' : '▶️ Run' }}
        </button>
      </div>
    </div>

    <!-- SQL Editor -->
    <div class="editor-container">
      <textarea 
        v-model="sqlQuery"
        class="sql-editor"
        placeholder="Enter your SQL query here...

Example:
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER LIMIT 10;

CREATE TABLE my_table (id INT, name VARCHAR);
INSERT INTO my_table VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM my_table;"
        @keydown.ctrl.enter="executeQuery"
        @keydown.meta.enter="executeQuery"
      ></textarea>
      
      <div class="editor-footer">
        <span class="editor-info">
          {{ sqlQuery.length }} characters | {{ lineCount }} lines
        </span>
        <span class="editor-tip">
          💡 Press Ctrl+Enter (⌘+Enter on Mac) to execute
        </span>
      </div>
    </div>

    <!-- Query Results -->
    <div v-if="lastResult" class="results-container">
      <div class="results-header">
        <h3>{{ lastResult.success ? '✅ Query Results' : '❌ Query Error' }}</h3>
        <div class="results-meta">
          <span v-if="lastResult.success">
            📊 {{ lastResult.rowcount }} row(s) | ⏱️ {{ lastResult.duration_ms.toFixed(2) }}ms
          </span>
          <button class="btn-small" @click="exportResults" v-if="lastResult.success && lastResult.data.length > 0">
            💾 Export CSV
          </button>
        </div>
      </div>

      <!-- Error Message -->
      <div v-if="!lastResult.success" class="error-message">
        <pre>{{ lastResult.error }}</pre>
      </div>

      <!-- Data Table -->
      <div v-else-if="lastResult.data.length > 0" class="table-wrapper">
        <table class="results-table">
          <thead>
            <tr>
              <th v-for="(col, idx) in lastResult.columns" :key="idx" class="column-header">
                {{ col }}
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, rowIdx) in displayedRows" :key="rowIdx">
              <td v-for="(col, colIdx) in lastResult.columns" :key="colIdx" class="data-cell">
                {{ formatCellValue(row[colIdx]) }}
              </td>
            </tr>
          </tbody>
        </table>

        <!-- Pagination -->
        <div v-if="lastResult.data.length > pageSize" class="pagination">
          <button 
            class="btn-small" 
            @click="currentPage = Math.max(1, currentPage - 1)"
            :disabled="currentPage === 1"
          >
            ◀️ Previous
          </button>
          <span class="page-info">
            Page {{ currentPage }} of {{ totalPages }} ({{ lastResult.data.length }} rows)
          </span>
          <button 
            class="btn-small" 
            @click="currentPage = Math.min(totalPages, currentPage + 1)"
            :disabled="currentPage === totalPages"
          >
            Next ▶️
          </button>
        </div>
      </div>

      <!-- No Results -->
      <div v-else class="no-results">
        <p>✅ Query executed successfully ({{ lastResult.rowcount }} row(s) affected)</p>
        <p class="muted">No data to display</p>
      </div>
    </div>

    <!-- Sample Queries -->
    <div class="sample-queries">
      <h3>📚 Sample Queries</h3>
      <div class="query-chips">
        <button 
          v-for="(sample, idx) in sampleQueries" 
          :key="idx"
          class="query-chip"
          @click="loadSample(sample.query)"
          :title="sample.description"
        >
          {{ sample.name }}
        </button>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'WorksheetPanel',
  data() {
    return {
      sqlQuery: '',
      lastResult: null,
      isExecuting: false,
      currentPage: 1,
      pageSize: 50,
      sampleQueries: [
        {
          name: '🗄️ Create Database',
          query: 'CREATE DATABASE IF NOT EXISTS my_database;',
          description: 'Create a new database'
        },
        {
          name: '📁 Create Schema',
          query: 'CREATE SCHEMA IF NOT EXISTS my_database.my_schema;',
          description: 'Create a schema in a database'
        },
        {
          name: '📊 Create Table',
          query: `CREATE TABLE IF NOT EXISTS customers (
  id INTEGER,
  name VARCHAR,
  email VARCHAR,
  created_at TIMESTAMP
);`,
          description: 'Create a table with various data types'
        },
        {
          name: '➕ Insert Data',
          query: `INSERT INTO customers VALUES
  (1, 'Alice Johnson', 'alice@example.com', CURRENT_TIMESTAMP),
  (2, 'Bob Smith', 'bob@example.com', CURRENT_TIMESTAMP),
  (3, 'Carol White', 'carol@example.com', CURRENT_TIMESTAMP);`,
          description: 'Insert sample data'
        },
        {
          name: '🔍 Select All',
          query: 'SELECT * FROM customers;',
          description: 'Query all data from a table'
        },
        {
          name: '📋 Show Objects',
          query: 'SHOW DATABASES;\n-- SHOW SCHEMAS;\n-- SHOW TABLES;',
          description: 'List databases, schemas, and tables'
        },
        {
          name: '📝 Describe Table',
          query: 'DESCRIBE TABLE customers;',
          description: 'Show table structure'
        }
      ]
    }
  },
  computed: {
    lineCount() {
      return this.sqlQuery.split('\n').length
    },
    displayedRows() {
      if (!this.lastResult || !this.lastResult.data) return []
      const start = (this.currentPage - 1) * this.pageSize
      const end = start + this.pageSize
      return this.lastResult.data.slice(start, end)
    },
    totalPages() {
      if (!this.lastResult || !this.lastResult.data) return 1
      return Math.ceil(this.lastResult.data.length / this.pageSize)
    }
  },
  methods: {
    async executeQuery() {
      if (!this.sqlQuery.trim() || this.isExecuting) return

      this.isExecuting = true
      this.lastResult = null
      this.currentPage = 1

      try {
        const response = await axios.post('/api/execute', {
          sql: this.sqlQuery
        })

        this.lastResult = response.data

        if (response.data.success) {
          this.$emit('query-executed', response.data)
        }
      } catch (error) {
        this.lastResult = {
          success: false,
          error: error.response?.data?.error || error.message || 'Failed to execute query'
        }
      } finally {
        this.isExecuting = false
      }
    },
    clearEditor() {
      if (confirm('Clear the SQL editor?')) {
        this.sqlQuery = ''
        this.lastResult = null
      }
    },
    formatSql() {
      // Basic SQL formatting
      let formatted = this.sqlQuery
        .replace(/\s+/g, ' ')
        .replace(/,/g, ',\n  ')
        .replace(/\bFROM\b/gi, '\nFROM')
        .replace(/\bWHERE\b/gi, '\nWHERE')
        .replace(/\bAND\b/gi, '\n  AND')
        .replace(/\bOR\b/gi, '\n  OR')
        .replace(/\bJOIN\b/gi, '\nJOIN')
        .replace(/\bINNER JOIN\b/gi, '\nINNER JOIN')
        .replace(/\bLEFT JOIN\b/gi, '\nLEFT JOIN')
        .replace(/\bRIGHT JOIN\b/gi, '\nRIGHT JOIN')
        .replace(/\bGROUP BY\b/gi, '\nGROUP BY')
        .replace(/\bORDER BY\b/gi, '\nORDER BY')
        .replace(/\bLIMIT\b/gi, '\nLIMIT')
        .trim()
      
      this.sqlQuery = formatted
    },
    loadSample(query) {
      this.sqlQuery = query
      this.lastResult = null
    },
    formatCellValue(value) {
      if (value === null || value === undefined) {
        return 'NULL'
      }
      if (typeof value === 'string' && value.length > 100) {
        return value.substring(0, 100) + '...'
      }
      return value
    },
    exportResults() {
      if (!this.lastResult || !this.lastResult.success) return

      // Generate CSV
      const rows = [
        this.lastResult.columns.join(','),
        ...this.lastResult.data.map(row => 
          row.map(cell => {
            const value = cell === null ? '' : String(cell)
            // Escape quotes and wrap in quotes if contains comma or quote
            if (value.includes(',') || value.includes('"') || value.includes('\n')) {
              return `"${value.replace(/"/g, '""')}"`
            }
            return value
          }).join(',')
        )
      ].join('\n')

      // Download
      const blob = new Blob([rows], { type: 'text/csv' })
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `query_results_${new Date().getTime()}.csv`
      a.click()
      window.URL.revokeObjectURL(url)
    }
  }
}
</script>

<style scoped>
.worksheet-panel {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.panel-header h2 {
  font-size: 1.5rem;
  margin: 0;
  color: var(--text-color);
}

.header-actions {
  display: flex;
  gap: 0.5rem;
}

.btn-primary, .btn-secondary, .btn-small {
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.9rem;
  font-weight: 500;
  transition: all 0.2s ease;
}

.btn-primary {
  background: var(--primary-color);
  color: white;
}

.btn-primary:hover:not(:disabled) {
  opacity: 0.9;
  transform: translateY(-1px);
}

.btn-primary:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.btn-secondary {
  background: var(--card-bg);
  color: var(--text-color);
  border: 1px solid var(--border-color);
}

.btn-secondary:hover {
  background: var(--bg-color);
}

.btn-small {
  padding: 0.375rem 0.75rem;
  font-size: 0.85rem;
  background: var(--card-bg);
  color: var(--text-color);
  border: 1px solid var(--border-color);
}

.btn-small:hover:not(:disabled) {
  background: var(--bg-color);
}

.btn-small:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.editor-container {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  overflow: hidden;
}

.sql-editor {
  width: 100%;
  min-height: 250px;
  padding: 1rem;
  border: none;
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 0.95rem;
  line-height: 1.6;
  resize: vertical;
  background: #1e1e1e;
  color: #d4d4d4;
  tab-size: 2;
}

.sql-editor::placeholder {
  color: #666;
}

.editor-footer {
  display: flex;
  justify-content: space-between;
  padding: 0.5rem 1rem;
  background: var(--bg-color);
  border-top: 1px solid var(--border-color);
  font-size: 0.8rem;
  color: var(--text-muted);
}

.editor-tip {
  font-style: italic;
}

.results-container {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  overflow: hidden;
}

.results-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem;
  background: var(--bg-color);
  border-bottom: 1px solid var(--border-color);
}

.results-header h3 {
  margin: 0;
  font-size: 1.1rem;
}

.results-meta {
  display: flex;
  align-items: center;
  gap: 1rem;
  font-size: 0.9rem;
  color: var(--text-muted);
}

.error-message {
  padding: 1.5rem;
  background: #fee;
  color: #c33;
}

.error-message pre {
  margin: 0;
  white-space: pre-wrap;
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 0.9rem;
}

.table-wrapper {
  overflow-x: auto;
}

.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9rem;
}

.column-header {
  background: var(--primary-color);
  color: white;
  padding: 0.75rem 1rem;
  text-align: left;
  font-weight: 600;
  white-space: nowrap;
  position: sticky;
  top: 0;
  z-index: 10;
}

.data-cell {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--border-color);
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.results-table tbody tr:hover {
  background: var(--bg-color);
}

.pagination {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem;
  background: var(--bg-color);
  border-top: 1px solid var(--border-color);
}

.page-info {
  font-size: 0.9rem;
  color: var(--text-muted);
}

.no-results {
  padding: 2rem;
  text-align: center;
  color: var(--text-muted);
}

.no-results p {
  margin: 0.5rem 0;
}

.muted {
  font-size: 0.9rem;
  opacity: 0.7;
}

.sample-queries {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 1rem;
}

.sample-queries h3 {
  font-size: 1.1rem;
  margin: 0 0 1rem 0;
}

.query-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.query-chip {
  padding: 0.5rem 1rem;
  background: var(--bg-color);
  border: 1px solid var(--border-color);
  border-radius: 20px;
  cursor: pointer;
  font-size: 0.85rem;
  transition: all 0.2s ease;
}

.query-chip:hover {
  background: var(--primary-color);
  color: white;
  border-color: var(--primary-color);
  transform: translateY(-2px);
}
</style>
