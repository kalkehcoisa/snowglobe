<template>
  <div class="worksheets-page">
    <!-- Loading State -->
    <div v-if="isLoading" class="loading-state">
      <span class="loading-spinner">⏳</span>
      <p>Loading worksheets...</p>
    </div>

    <template v-else>
      <!-- Worksheet Tabs -->
      <div class="worksheet-tabs">
        <div class="tabs-container" ref="tabsContainer">
          <div 
            v-for="(ws, index) in worksheets" 
            :key="ws.id"
            :class="['worksheet-tab', { active: activeWorksheetId === ws.id, dragging: draggedIndex === index }]"
            :draggable="true"
            @click="selectWorksheet(ws.id)"
            @dragstart="handleDragStart($event, index)"
            @dragover="handleDragOver($event, index)"
            @dragend="handleDragEnd"
            @drop="handleDrop($event, index)"
          >
            <span class="drag-handle" title="Drag to reorder">⋮⋮</span>
            <span class="tab-icon">{{ ws.is_favorite ? '⭐' : '📝' }}</span>
            <input 
              v-if="ws.isEditing"
              v-model="ws.name"
              @blur="finishEditing(ws)"
              @keyup.enter="finishEditing(ws)"
              class="tab-name-input"
              ref="tabInput"
            />
            <span v-else class="tab-name" @dblclick="startEditing(ws)">{{ ws.name }}</span>
            <button 
              class="tab-close" 
              @click.stop="closeWorksheet(index)"
              title="Close worksheet"
              v-if="worksheets.length > 1"
            >
              ×
            </button>
          </div>
        </div>
        <div class="tabs-actions">
          <span v-if="isSaving" class="save-indicator">💾</span>
          <button class="btn-icon-small" @click="toggleFavorite" title="Toggle Favorite" v-if="activeWorksheet">
            {{ activeWorksheet.is_favorite ? '⭐' : '☆' }}
          </button>
          <button class="btn-icon-small" @click="duplicateWorksheet" title="Duplicate" v-if="activeWorksheet">
            📋
          </button>
          <button class="add-worksheet-btn" @click="addWorksheet" title="New Worksheet">
            <span>+</span>
          </button>
        </div>
      </div>

    <!-- Active Worksheet Content -->
    <div class="worksheet-content" v-if="activeWorksheet">
      <!-- Context Bar -->
      <div class="context-bar">
        <div class="context-item">
          <label>Database:</label>
          <select v-model="activeWorksheet.context.database" @change="onContextChange">
            <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
          </select>
        </div>
        <div class="context-item">
          <label>Schema:</label>
          <select v-model="activeWorksheet.context.schema">
            <option value="PUBLIC">PUBLIC</option>
            <option value="INFORMATION_SCHEMA">INFORMATION_SCHEMA</option>
          </select>
        </div>
        <div class="context-item">
          <label>Warehouse:</label>
          <select v-model="activeWorksheet.context.warehouse">
            <option value="COMPUTE_WH">COMPUTE_WH</option>
          </select>
        </div>
        <div class="context-item">
          <label>Role:</label>
          <select v-model="activeWorksheet.context.role">
            <option value="ACCOUNTADMIN">ACCOUNTADMIN</option>
            <option value="SYSADMIN">SYSADMIN</option>
            <option value="PUBLIC">PUBLIC</option>
          </select>
        </div>
      </div>

      <!-- Toolbar -->
      <div class="toolbar">
        <div class="toolbar-left">
          <button class="btn-primary" @click="executeQuery" :disabled="isExecuting">
            <span v-if="isExecuting">⏳</span>
            <span v-else>▶️</span>
            {{ isExecuting ? 'Running...' : 'Run' }}
          </button>
          <button class="btn-secondary" @click="executeSelectedQuery" :disabled="isExecuting || !hasSelection" title="Run selected query (Ctrl+Enter)">
            Run Selection
          </button>
        </div>
        <div class="toolbar-right">
          <button class="btn-icon" @click="formatSql" title="Format SQL">
            ✨
          </button>
          <button class="btn-icon" @click="clearEditor" title="Clear">
            🗑️
          </button>
          <button class="btn-icon" @click="openHistory" title="Query History">
            🕒
          </button>
        </div>
      </div>

      <!-- SQL Editor -->
      <div class="editor-section">
        <textarea 
          v-model="activeWorksheet.sql"
          class="sql-editor"
          placeholder="-- Enter your SQL query here
-- Press Ctrl+Enter to execute
-- Double-click tab to rename

SELECT * FROM my_table LIMIT 100;"
          @keydown.ctrl.enter="executeQuery"
          @keydown.meta.enter="executeQuery"
          @select="onSelect"
          ref="sqlEditor"
        ></textarea>
        <div class="editor-status">
          <span>{{ activeWorksheet.sql.length }} chars | {{ lineCount }} lines</span>
          <span v-if="activeWorksheet.lastExecuted">
            Last run: {{ formatTime(activeWorksheet.lastExecuted) }}
          </span>
        </div>
      </div>

      <!-- Results Section -->
      <div class="results-section" v-if="activeWorksheet.result">
        <div class="results-header">
          <div class="results-title">
            <span v-if="activeWorksheet.result.success" class="success-icon">✅</span>
            <span v-else class="error-icon">❌</span>
            <span>Results</span>
          </div>
          <div class="results-info" v-if="activeWorksheet.result.success">
            <span>{{ activeWorksheet.result.rowcount }} rows</span>
            <span>•</span>
            <span>{{ activeWorksheet.result.duration_ms?.toFixed(2) || 0 }}ms</span>
            <button class="btn-small" @click="exportCsv" v-if="activeWorksheet.result.data?.length">
              📥 Export
            </button>
          </div>
        </div>

        <!-- Error Display -->
        <div v-if="!activeWorksheet.result.success" class="error-panel">
          <pre>{{ activeWorksheet.result.error }}</pre>
        </div>

        <!-- Data Table -->
        <div v-else-if="activeWorksheet.result.data?.length > 0" class="table-container">
          <table class="results-table">
            <thead>
              <tr>
                <th class="row-num-header">#</th>
                <th v-for="col in activeWorksheet.result.columns" :key="col">
                  {{ col }}
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, idx) in paginatedData" :key="idx">
                <td class="row-num">{{ (currentPage - 1) * pageSize + idx + 1 }}</td>
                <td v-for="(col, colIdx) in activeWorksheet.result.columns" :key="colIdx">
                  <span :class="getCellClass(row[colIdx])">
                    {{ formatCell(row[colIdx]) }}
                  </span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- No Data -->
        <div v-else class="no-data">
          <p>✅ Query executed successfully</p>
          <p class="muted">{{ activeWorksheet.result.rowcount }} row(s) affected</p>
        </div>

        <!-- Pagination -->
        <div v-if="totalPages > 1" class="pagination">
          <button @click="currentPage = 1" :disabled="currentPage === 1">⏮️</button>
          <button @click="currentPage--" :disabled="currentPage === 1">◀️</button>
          <span>Page {{ currentPage }} of {{ totalPages }}</span>
          <button @click="currentPage++" :disabled="currentPage === totalPages">▶️</button>
          <button @click="currentPage = totalPages" :disabled="currentPage === totalPages">⏭️</button>
        </div>
      </div>

      <!-- Quick Actions -->
      <div class="quick-actions" v-if="!activeWorksheet.result">
        <h4>Quick Templates</h4>
        <div class="action-buttons">
          <button @click="insertTemplate('select')">SELECT</button>
          <button @click="insertTemplate('create')">CREATE TABLE</button>
          <button @click="insertTemplate('insert')">INSERT</button>
          <button @click="insertTemplate('show')">SHOW</button>
          <button @click="insertTemplate('describe')">DESCRIBE</button>
        </div>
      </div>
    </div>
    </template>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'WorksheetsPage',
  props: {
    databases: {
      type: Array,
      default: () => []
    }
  },
  data() {
    return {
      worksheets: [],
      activeWorksheetId: null,
      isExecuting: false,
      hasSelection: false,
      currentPage: 1,
      pageSize: 100,
      isLoading: true,
      saveTimeout: null,
      isSaving: false,
      // Drag and drop for reordering
      draggedIndex: null,
      dragOverIndex: null
    }
  },
  computed: {
    activeWorksheet() {
      return this.worksheets.find(ws => ws.id === this.activeWorksheetId)
    },
    lineCount() {
      return this.activeWorksheet?.sql?.split('\n').length || 0
    },
    paginatedData() {
      if (!this.activeWorksheet?.result?.data) return []
      const start = (this.currentPage - 1) * this.pageSize
      return this.activeWorksheet.result.data.slice(start, start + this.pageSize)
    },
    totalPages() {
      if (!this.activeWorksheet?.result?.data) return 1
      return Math.ceil(this.activeWorksheet.result.data.length / this.pageSize)
    }
  },
  watch: {
    worksheets: {
      handler() {
        this.$emit('worksheet-count-changed', this.worksheets.length)
      },
      deep: true
    },
    'activeWorksheet.sql': {
      handler() {
        // Auto-save SQL changes with debounce
        this.debounceSave()
      }
    }
  },
  methods: {
    async fetchWorksheets() {
      this.isLoading = true
      try {
        const response = await axios.get('/api/worksheets')
        // Worksheets are already sorted by position from server
        const serverWorksheets = response.data.worksheets.map(ws => ({
          ...ws,
          result: null,
          isEditing: false
        }))
        
        if (serverWorksheets.length > 0) {
          this.worksheets = serverWorksheets
          // Keep active worksheet if it still exists, otherwise select first
          if (!this.activeWorksheetId || !serverWorksheets.find(ws => ws.id === this.activeWorksheetId)) {
            this.activeWorksheetId = serverWorksheets[0].id
          }
        } else {
          // Create a default worksheet if none exist
          await this.addWorksheet()
        }
      } catch (error) {
        console.error('Failed to fetch worksheets:', error)
        // Create a local worksheet if backend fails
        this.worksheets = [{
          id: 'local_1',
          name: 'Worksheet 1',
          sql: '',
          result: null,
          context: {
            database: 'SNOWGLOBE',
            schema: 'PUBLIC',
            warehouse: 'COMPUTE_WH',
            role: 'ACCOUNTADMIN'
          },
          position: 0,
          lastExecuted: null,
          isEditing: false,
          is_favorite: false
        }]
        this.activeWorksheetId = 'local_1'
      } finally {
        this.isLoading = false
      }
    },
    
    async addWorksheet() {
      try {
        const response = await axios.post('/api/worksheets', {
          name: `Worksheet ${this.worksheets.length + 1}`,
          sql: '',
          context: {
            database: 'SNOWGLOBE',
            schema: 'PUBLIC',
            warehouse: 'COMPUTE_WH',
            role: 'ACCOUNTADMIN'
          }
        })
        
        const newWorksheet = {
          ...response.data.worksheet,
          result: null,
          isEditing: false
        }
        this.worksheets.push(newWorksheet)
        this.activeWorksheetId = newWorksheet.id
      } catch (error) {
        console.error('Failed to create worksheet:', error)
        // Create locally if backend fails
        const localId = `local_${Date.now()}`
        const newWorksheet = {
          id: localId,
          name: `Worksheet ${this.worksheets.length + 1}`,
          sql: '',
          result: null,
          context: {
            database: 'SNOWGLOBE',
            schema: 'PUBLIC',
            warehouse: 'COMPUTE_WH',
            role: 'ACCOUNTADMIN'
          },
          lastExecuted: null,
          isEditing: false
        }
        this.worksheets.push(newWorksheet)
        this.activeWorksheetId = localId
      }
    },
    
    async closeWorksheet(index) {
      if (this.worksheets.length <= 1) return
      
      const ws = this.worksheets[index]
      if (ws.sql && !confirm(`Close "${ws.name}"? Unsaved changes will be lost.`)) {
        return
      }
      
      try {
        // Delete from backend
        if (!ws.id.startsWith('local_')) {
          await axios.delete(`/api/worksheets/${ws.id}`)
        }
      } catch (error) {
        console.error('Failed to delete worksheet:', error)
      }
      
      this.worksheets.splice(index, 1)
      if (this.activeWorksheetId === ws.id) {
        this.activeWorksheetId = this.worksheets[Math.max(0, index - 1)].id
      }
    },
    
    selectWorksheet(id) {
      this.activeWorksheetId = id
      this.currentPage = 1
    },
    
    startEditing(ws) {
      ws.isEditing = true
      this.$nextTick(() => {
        const input = this.$refs.tabInput?.[0]
        if (input) input.focus()
      })
    },
    
    async finishEditing(ws) {
      ws.isEditing = false
      if (!ws.name.trim()) {
        ws.name = `Worksheet ${ws.id}`
      }
      // Save name change to backend
      await this.saveWorksheet(ws)
    },
    
    debounceSave() {
      if (this.saveTimeout) {
        clearTimeout(this.saveTimeout)
      }
      this.saveTimeout = setTimeout(() => {
        if (this.activeWorksheet) {
          this.saveWorksheet(this.activeWorksheet)
        }
      }, 1000)
    },
    
    async saveWorksheet(ws) {
      if (ws.id.startsWith('local_')) return
      
      this.isSaving = true
      try {
        await axios.put(`/api/worksheets/${ws.id}`, {
          name: ws.name,
          sql: ws.sql,
          context: ws.context
        })
      } catch (error) {
        console.error('Failed to save worksheet:', error)
      } finally {
        this.isSaving = false
      }
    },
    
    async executeQuery() {
      if (!this.activeWorksheet?.sql.trim() || this.isExecuting) return
      
      this.isExecuting = true
      this.activeWorksheet.result = null
      this.currentPage = 1
      
      try {
        const response = await axios.post('/api/execute', {
          sql: this.activeWorksheet.sql
        })
        
        this.activeWorksheet.result = response.data
        this.activeWorksheet.lastExecuted = new Date()
        
        // Save last executed time to backend
        if (!this.activeWorksheet.id.startsWith('local_')) {
          await this.saveWorksheet(this.activeWorksheet)
        }
        
        if (response.data.success) {
          this.$emit('query-executed', response.data)
        }
      } catch (error) {
        this.activeWorksheet.result = {
          success: false,
          error: error.response?.data?.error || error.message
        }
      } finally {
        this.isExecuting = false
      }
    },
    
    executeSelectedQuery() {
      // Get selected text from textarea
      const editor = this.$refs.sqlEditor
      if (editor && editor.selectionStart !== editor.selectionEnd) {
        const selected = this.activeWorksheet.sql.substring(
          editor.selectionStart,
          editor.selectionEnd
        )
        if (selected.trim()) {
          const originalSql = this.activeWorksheet.sql
          this.activeWorksheet.sql = selected
          this.executeQuery().then(() => {
            this.activeWorksheet.sql = originalSql
          })
        }
      }
    },
    
    onSelect() {
      const editor = this.$refs.sqlEditor
      this.hasSelection = editor && editor.selectionStart !== editor.selectionEnd
    },
    
    formatSql() {
      if (!this.activeWorksheet) return
      
      let sql = this.activeWorksheet.sql
        .replace(/\s+/g, ' ')
        .replace(/,\s*/g, ',\n  ')
        .replace(/\bFROM\b/gi, '\nFROM')
        .replace(/\bWHERE\b/gi, '\nWHERE')
        .replace(/\bAND\b/gi, '\n  AND')
        .replace(/\bOR\b/gi, '\n  OR')
        .replace(/\bJOIN\b/gi, '\nJOIN')
        .replace(/\bGROUP BY\b/gi, '\nGROUP BY')
        .replace(/\bORDER BY\b/gi, '\nORDER BY')
        .replace(/\bLIMIT\b/gi, '\nLIMIT')
        .trim()
      
      this.activeWorksheet.sql = sql
    },
    
    clearEditor() {
      if (this.activeWorksheet?.sql && confirm('Clear the editor?')) {
        this.activeWorksheet.sql = ''
        this.activeWorksheet.result = null
      }
    },
    
    openHistory() {
      this.$emit('navigate', 'queries')
    },
    
    onContextChange() {
      // Save context change to backend
      if (this.activeWorksheet) {
        this.saveWorksheet(this.activeWorksheet)
      }
    },
    
    insertTemplate(type) {
      const templates = {
        select: 'SELECT *\nFROM table_name\nWHERE condition\nLIMIT 100;',
        create: 'CREATE TABLE IF NOT EXISTS table_name (\n  id INTEGER,\n  name VARCHAR,\n  created_at TIMESTAMP\n);',
        insert: "INSERT INTO table_name (col1, col2)\nVALUES ('value1', 'value2');",
        show: 'SHOW DATABASES;\n-- SHOW SCHEMAS;\n-- SHOW TABLES;',
        describe: 'DESCRIBE TABLE table_name;'
      }
      
      if (this.activeWorksheet) {
        this.activeWorksheet.sql = templates[type] || ''
      }
    },
    
    formatCell(value) {
      if (value === null || value === undefined) return 'NULL'
      if (typeof value === 'string' && value.length > 50) {
        return value.substring(0, 50) + '...'
      }
      return value
    },
    
    getCellClass(value) {
      if (value === null || value === undefined) return 'null-value'
      if (typeof value === 'number') return 'number-value'
      return ''
    },
    
    formatTime(date) {
      return new Date(date).toLocaleTimeString()
    },
    
    exportCsv() {
      if (!this.activeWorksheet?.result?.data) return
      
      const { columns, data } = this.activeWorksheet.result
      const rows = [
        columns.join(','),
        ...data.map(row => 
          row.map(cell => {
            const val = cell === null ? '' : String(cell)
            return val.includes(',') || val.includes('"') || val.includes('\n')
              ? `"${val.replace(/"/g, '""')}"`
              : val
          }).join(',')
        )
      ].join('\n')
      
      const blob = new Blob([rows], { type: 'text/csv' })
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${this.activeWorksheet.name}_${Date.now()}.csv`
      a.click()
      window.URL.revokeObjectURL(url)
    },
    
    // Drag and drop for reordering worksheets
    handleDragStart(event, index) {
      this.draggedIndex = index
      event.dataTransfer.effectAllowed = 'move'
      event.dataTransfer.setData('text/plain', index)
      // Add visual feedback
      event.target.classList.add('dragging')
    },
    
    handleDragOver(event, index) {
      event.preventDefault()
      event.dataTransfer.dropEffect = 'move'
      this.dragOverIndex = index
    },
    
    handleDragEnd() {
      this.draggedIndex = null
      this.dragOverIndex = null
    },
    
    async handleDrop(event, dropIndex) {
      event.preventDefault()
      
      if (this.draggedIndex === null || this.draggedIndex === dropIndex) {
        return
      }
      
      // Reorder worksheets locally
      const worksheetsCopy = [...this.worksheets]
      const [draggedItem] = worksheetsCopy.splice(this.draggedIndex, 1)
      worksheetsCopy.splice(dropIndex, 0, draggedItem)
      
      // Update positions
      worksheetsCopy.forEach((ws, idx) => {
        ws.position = idx
      })
      
      this.worksheets = worksheetsCopy
      
      // Save new order to server
      try {
        await axios.post('/api/worksheets/reorder', {
          worksheet_ids: this.worksheets.map(ws => ws.id)
        })
      } catch (error) {
        console.error('Failed to save worksheet order:', error)
      }
      
      this.draggedIndex = null
      this.dragOverIndex = null
    },
    
    async toggleFavorite() {
      if (!this.activeWorksheet || this.activeWorksheet.id.startsWith('local_')) return
      
      try {
        const response = await axios.post(`/api/worksheets/${this.activeWorksheet.id}/favorite`)
        this.activeWorksheet.is_favorite = response.data.is_favorite
      } catch (error) {
        console.error('Failed to toggle favorite:', error)
      }
    },
    
    async duplicateWorksheet() {
      if (!this.activeWorksheet) return
      
      try {
        const response = await axios.post(`/api/worksheets/${this.activeWorksheet.id}/duplicate`)
        const newWorksheet = {
          ...response.data.worksheet,
          result: null,
          isEditing: false
        }
        this.worksheets.push(newWorksheet)
        this.activeWorksheetId = newWorksheet.id
      } catch (error) {
        console.error('Failed to duplicate worksheet:', error)
        // Fallback to local duplicate
        const copy = {
          id: `local_${Date.now()}`,
          name: `Copy of ${this.activeWorksheet.name}`,
          sql: this.activeWorksheet.sql,
          context: { ...this.activeWorksheet.context },
          result: null,
          isEditing: false,
          position: this.worksheets.length,
          is_favorite: false
        }
        this.worksheets.push(copy)
        this.activeWorksheetId = copy.id
      }
    }
  },
  async mounted() {
    await this.fetchWorksheets()
    this.$emit('worksheet-count-changed', this.worksheets.length)
  },
  beforeUnmount() {
    if (this.saveTimeout) {
      clearTimeout(this.saveTimeout)
    }
  }
}
</script>

<style scoped>
.worksheets-page {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--bg-color);
}

.loading-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 200px;
  color: var(--text-muted);
}

.loading-spinner {
  font-size: 2rem;
  animation: pulse 1s ease-in-out infinite;
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}

.worksheet-tabs {
  display: flex;
  align-items: center;
  background: var(--card-bg);
  border-bottom: 1px solid var(--border-color);
  padding: 0.5rem 0.5rem 0;
}

.tabs-actions {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-left: 0.5rem;
}

.save-indicator {
  font-size: 0.9rem;
  animation: pulse 1s ease-in-out infinite;
}

.tabs-container {
  display: flex;
  flex: 1;
  overflow-x: auto;
  gap: 0.25rem;
}

.worksheet-tab {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  background: var(--bg-color);
  border: 1px solid var(--border-color);
  border-bottom: none;
  border-radius: 6px 6px 0 0;
  cursor: pointer;
  font-size: 0.85rem;
  white-space: nowrap;
  min-width: 120px;
  max-width: 200px;
  transition: transform 0.2s, opacity 0.2s;
}

.worksheet-tab.dragging {
  opacity: 0.5;
  transform: scale(0.95);
}

.drag-handle {
  cursor: grab;
  color: var(--text-muted);
  font-size: 0.75rem;
  padding: 0 0.25rem;
  user-select: none;
}

.drag-handle:active {
  cursor: grabbing;
}

.btn-icon-small {
  background: none;
  border: none;
  cursor: pointer;
  font-size: 1rem;
  padding: 0.25rem;
  opacity: 0.7;
  transition: opacity 0.2s;
}

.btn-icon-small:hover {
  opacity: 1;
}

.worksheet-tab.active {
  background: var(--card-bg);
  border-bottom: 2px solid var(--card-bg);
  margin-bottom: -1px;
}

.worksheet-tab:hover:not(.active) {
  background: var(--border-color);
}

.tab-icon {
  font-size: 1rem;
}

.tab-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
}

.tab-name-input {
  flex: 1;
  background: transparent;
  border: none;
  font-size: inherit;
  padding: 0;
  outline: none;
  width: 100%;
}

.tab-close {
  background: none;
  border: none;
  cursor: pointer;
  font-size: 1.2rem;
  color: var(--text-muted);
  padding: 0;
  line-height: 1;
}

.tab-close:hover {
  color: var(--error-color, #ef4444);
}

.add-worksheet-btn {
  background: none;
  border: 1px dashed var(--border-color);
  border-radius: 6px;
  width: 32px;
  height: 32px;
  cursor: pointer;
  font-size: 1.25rem;
  color: var(--text-muted);
  margin-left: 0.5rem;
}

.add-worksheet-btn:hover {
  background: var(--bg-color);
  color: var(--primary-color);
  border-color: var(--primary-color);
}

.worksheet-content {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  padding: 1rem;
  gap: 0.75rem;
}

.context-bar {
  display: flex;
  gap: 1rem;
  padding: 0.75rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
}

.context-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.context-item label {
  font-size: 0.8rem;
  color: var(--text-muted);
}

.context-item select {
  padding: 0.375rem 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--bg-color);
  font-size: 0.85rem;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.toolbar-left, .toolbar-right {
  display: flex;
  gap: 0.5rem;
}

.btn-primary, .btn-secondary {
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.9rem;
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 0.375rem;
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
  background: var(--card-bg);
  color: var(--text-color);
  border: 1px solid var(--border-color);
}

.btn-secondary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-icon {
  padding: 0.5rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  cursor: pointer;
  font-size: 1rem;
}

.btn-icon:hover {
  background: var(--bg-color);
}

.editor-section {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 200px;
  max-height: 40vh;
}

.sql-editor {
  flex: 1;
  padding: 1rem;
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 0.95rem;
  line-height: 1.5;
  border: 1px solid var(--border-color);
  border-radius: 6px 6px 0 0;
  resize: none;
  background: #1e1e1e;
  color: #d4d4d4;
}

.editor-status {
  display: flex;
  justify-content: space-between;
  padding: 0.5rem 1rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-top: none;
  border-radius: 0 0 6px 6px;
  font-size: 0.8rem;
  color: var(--text-muted);
}

.results-section {
  flex: 1;
  display: flex;
  flex-direction: column;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  overflow: hidden;
}

.results-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem 1rem;
  background: var(--bg-color);
  border-bottom: 1px solid var(--border-color);
}

.results-title {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 500;
}

.success-icon {
  color: #10b981;
}

.error-icon {
  color: #ef4444;
}

.results-info {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  font-size: 0.85rem;
  color: var(--text-muted);
}

.btn-small {
  padding: 0.25rem 0.5rem;
  font-size: 0.8rem;
  background: var(--bg-color);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: pointer;
}

.error-panel {
  padding: 1rem;
  background: #fee;
  color: #c33;
  overflow: auto;
}

.error-panel pre {
  margin: 0;
  white-space: pre-wrap;
  font-family: monospace;
  font-size: 0.9rem;
}

.table-container {
  flex: 1;
  overflow: auto;
}

.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}

.results-table th {
  background: var(--primary-color);
  color: white;
  padding: 0.625rem 0.75rem;
  text-align: left;
  font-weight: 500;
  position: sticky;
  top: 0;
  white-space: nowrap;
}

.row-num-header, .row-num {
  background: var(--bg-color);
  color: var(--text-muted);
  font-size: 0.75rem;
  text-align: center;
  width: 50px;
  border-right: 1px solid var(--border-color);
}

.results-table td {
  padding: 0.5rem 0.75rem;
  border-bottom: 1px solid var(--border-color);
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.results-table tr:hover td {
  background: var(--bg-color);
}

.null-value {
  color: var(--text-muted);
  font-style: italic;
}

.number-value {
  font-family: monospace;
  text-align: right;
}

.no-data {
  padding: 2rem;
  text-align: center;
  color: var(--text-muted);
}

.no-data .muted {
  font-size: 0.9rem;
  margin-top: 0.5rem;
}

.pagination {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 0.5rem;
  padding: 0.75rem;
  background: var(--bg-color);
  border-top: 1px solid var(--border-color);
}

.pagination button {
  padding: 0.375rem 0.5rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: pointer;
}

.pagination button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.quick-actions {
  padding: 1rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
}

.quick-actions h4 {
  margin: 0 0 0.75rem 0;
  font-size: 0.9rem;
  color: var(--text-muted);
}

.action-buttons {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.action-buttons button {
  padding: 0.5rem 1rem;
  background: var(--bg-color);
  border: 1px solid var(--border-color);
  border-radius: 20px;
  cursor: pointer;
  font-size: 0.85rem;
  transition: all 0.2s ease;
}

.action-buttons button:hover {
  background: var(--primary-color);
  color: white;
  border-color: var(--primary-color);
}
</style>
