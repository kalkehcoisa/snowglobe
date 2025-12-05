<template>
  <div class="python-worksheet">
    <!-- Toolbar -->
    <div class="toolbar">
      <div class="toolbar-left">
        <button class="btn-primary" @click="executeCode" :disabled="isExecuting">
          <span v-if="isExecuting">⏳</span>
          <span v-else>▶️</span>
          {{ isExecuting ? 'Running...' : 'Run' }}
        </button>
        <button class="btn-secondary" @click="validateCode" :disabled="isExecuting">
          ✓ Validate
        </button>
        <button class="btn-secondary" @click="showTemplates = true">
          📋 Templates
        </button>
      </div>
      <div class="toolbar-right">
        <button class="btn-icon" @click="formatCode" title="Format Code">
          ✨
        </button>
        <button class="btn-icon" @click="clearEditor" title="Clear">
          🗑️
        </button>
        <button class="btn-icon" @click="downloadCode" title="Download">
          💾
        </button>
      </div>
    </div>

    <!-- Editor Section -->
    <div class="editor-section">
      <div class="editor-header">
        <span class="language-badge">🐍 Python</span>
        <span class="status-text" v-if="validationResult">
          <span v-if="validationResult.valid" class="valid">✓ Valid</span>
          <span v-else class="invalid">✗ {{ validationResult.error }}</span>
        </span>
      </div>
      <textarea 
        v-model="code"
        class="code-editor"
        placeholder="# Python Worksheet with Snowpark-like APIs
# Available objects: session, col, lit, DataFrame, Row
# Example:
df = session.sql('SELECT * FROM my_table LIMIT 10')
print(df.show())
"
        @keydown.ctrl.enter="executeCode"
        @keydown.meta.enter="executeCode"
        @input="onCodeChange"
        spellcheck="false"
      ></textarea>
      <div class="editor-status">
        <span>{{ code.length }} chars | {{ lineCount }} lines</span>
        <span v-if="lastExecuted">Last run: {{ formatTime(lastExecuted) }}</span>
      </div>
    </div>

    <!-- Results Section -->
    <div class="results-section" v-if="result">
      <div class="results-header">
        <div class="results-title">
          <span v-if="result.success" class="success-icon">✅</span>
          <span v-else class="error-icon">❌</span>
          <span>Output</span>
        </div>
        <div class="results-info" v-if="result.success">
          <span>⏱️ {{ result.duration_ms?.toFixed(2) || 0 }}ms</span>
        </div>
      </div>

      <!-- Error Display -->
      <div v-if="!result.success" class="error-panel">
        <pre>{{ result.error }}</pre>
        <pre v-if="result.output" class="traceback">{{ result.output }}</pre>
      </div>

      <!-- Success Output -->
      <div v-else class="output-panel">
        <!-- Console Output -->
        <div v-if="result.output" class="console-output">
          <h4>Console Output</h4>
          <pre>{{ result.output }}</pre>
        </div>

        <!-- Variables -->
        <div v-if="Object.keys(result.variables || {}).length > 0" class="variables-section">
          <h4>Variables</h4>
          <table class="variables-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(value, name) in result.variables" :key="name">
                <td class="var-name">{{ name }}</td>
                <td class="var-value">{{ formatValue(value) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- DataFrames -->
        <div v-if="result.dataframes?.length > 0" class="dataframes-section">
          <h4>DataFrames</h4>
          <div v-for="df in result.dataframes" :key="df.name" class="dataframe-card">
            <div class="df-header">
              <span class="df-name">{{ df.name }}</span>
              <span class="df-info" v-if="!df.error">
                {{ df.total_rows }} rows × {{ df.columns?.length || 0 }} columns
              </span>
            </div>
            <div v-if="df.error" class="df-error">
              Error: {{ df.error }}
            </div>
            <pre v-else class="df-preview">{{ df.preview }}</pre>
          </div>
        </div>

        <!-- No output message -->
        <div v-if="!result.output && Object.keys(result.variables || {}).length === 0 && !result.dataframes?.length" class="no-output">
          <p>✅ Code executed successfully (no output)</p>
        </div>
      </div>
    </div>

    <!-- Templates Modal -->
    <div v-if="showTemplates" class="modal-overlay" @click.self="showTemplates = false">
      <div class="modal-content">
        <div class="modal-header">
          <h3>Python Templates</h3>
          <button class="close-button" @click="showTemplates = false">×</button>
        </div>
        <div class="modal-body">
          <div 
            v-for="template in templates" 
            :key="template.id"
            class="template-card"
            @click="applyTemplate(template)"
          >
            <div class="template-name">{{ template.name }}</div>
            <pre class="template-preview">{{ template.code.substring(0, 150) }}...</pre>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'PythonWorksheet',
  props: {
    worksheetId: {
      type: String,
      default: null
    },
    initialCode: {
      type: String,
      default: ''
    }
  },
  data() {
    return {
      code: this.initialCode,
      isExecuting: false,
      result: null,
      validationResult: null,
      lastExecuted: null,
      templates: [],
      showTemplates: false,
      validateTimeout: null
    }
  },
  computed: {
    lineCount() {
      return this.code.split('\n').length
    }
  },
  methods: {
    async executeCode() {
      if (!this.code.trim() || this.isExecuting) return
      
      this.isExecuting = true
      this.result = null
      
      try {
        const response = await axios.post('/api/python-worksheets/execute', {
          code: this.code,
          context: {}
        })
        
        this.result = response.data
        this.lastExecuted = new Date()
        
        if (response.data.success) {
          this.$emit('execution-success', response.data)
        }
      } catch (error) {
        this.result = {
          success: false,
          error: error.response?.data?.error || error.message
        }
      } finally {
        this.isExecuting = false
      }
    },
    
    async validateCode() {
      if (!this.code.trim()) return
      
      try {
        const response = await axios.post('/api/python-worksheets/validate', {
          code: this.code
        })
        this.validationResult = response.data
      } catch (error) {
        this.validationResult = {
          valid: false,
          error: error.message
        }
      }
    },
    
    onCodeChange() {
      this.validationResult = null
      
      // Debounced validation
      if (this.validateTimeout) {
        clearTimeout(this.validateTimeout)
      }
      this.validateTimeout = setTimeout(() => {
        if (this.code.trim()) {
          this.validateCode()
        }
      }, 1000)
    },
    
    async fetchTemplates() {
      try {
        const response = await axios.get('/api/python-worksheets/templates')
        this.templates = response.data.templates
      } catch (error) {
        console.error('Failed to fetch templates:', error)
      }
    },
    
    applyTemplate(template) {
      this.code = template.code
      this.showTemplates = false
      this.validationResult = null
    },
    
    formatCode() {
      // Basic Python formatting
      let lines = this.code.split('\n')
      let formatted = []
      let indentLevel = 0
      
      for (let line of lines) {
        let trimmed = line.trim()
        
        // Decrease indent for certain keywords
        if (trimmed.match(/^(elif|else|except|finally|case)\b/) || 
            trimmed.match(/^(return|break|continue|pass|raise)\b/)) {
          // Keep current indent
        } else if (trimmed.startsWith('}') || trimmed.startsWith(']') || trimmed.startsWith(')')) {
          indentLevel = Math.max(0, indentLevel - 1)
        }
        
        // Add the line with proper indentation
        if (trimmed) {
          formatted.push('    '.repeat(indentLevel) + trimmed)
        } else {
          formatted.push('')
        }
        
        // Increase indent after colons
        if (trimmed.endsWith(':')) {
          indentLevel++
        }
        // Decrease after certain statements
        if (trimmed.match(/^(return|break|continue|pass|raise)\b/) && !trimmed.endsWith(':')) {
          indentLevel = Math.max(0, indentLevel - 1)
        }
      }
      
      this.code = formatted.join('\n')
    },
    
    clearEditor() {
      if (this.code && confirm('Clear the editor?')) {
        this.code = ''
        this.result = null
        this.validationResult = null
      }
    },
    
    downloadCode() {
      const blob = new Blob([this.code], { type: 'text/x-python' })
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `python_worksheet_${Date.now()}.py`
      a.click()
      window.URL.revokeObjectURL(url)
    },
    
    formatValue(value) {
      if (value === null || value === undefined) return 'None'
      if (typeof value === 'object') {
        return JSON.stringify(value, null, 2)
      }
      return String(value)
    },
    
    formatTime(date) {
      return new Date(date).toLocaleTimeString()
    }
  },
  mounted() {
    this.fetchTemplates()
    
    // Load saved code if worksheet ID provided
    if (this.worksheetId) {
      // Load from workspace
    }
  },
  beforeUnmount() {
    if (this.validateTimeout) {
      clearTimeout(this.validateTimeout)
    }
  }
}
</script>

<style scoped>
.python-worksheet {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--bg-color);
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem;
  background: var(--card-bg);
  border-bottom: 1px solid var(--border-color);
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
  background: #3572A5; /* Python blue */
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

.btn-icon {
  padding: 0.5rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  cursor: pointer;
  font-size: 1rem;
}

.editor-section {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 200px;
  max-height: 50vh;
  margin: 0.75rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  overflow: hidden;
}

.editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.5rem 1rem;
  background: var(--bg-color);
  border-bottom: 1px solid var(--border-color);
}

.language-badge {
  background: #3572A5;
  color: white;
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
  font-size: 0.8rem;
  font-weight: 500;
}

.status-text {
  font-size: 0.85rem;
}

.status-text .valid {
  color: #10b981;
}

.status-text .invalid {
  color: #ef4444;
}

.code-editor {
  flex: 1;
  padding: 1rem;
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 0.95rem;
  line-height: 1.5;
  border: none;
  resize: none;
  background: #1e1e1e;
  color: #d4d4d4;
}

.code-editor:focus {
  outline: none;
}

.editor-status {
  display: flex;
  justify-content: space-between;
  padding: 0.5rem 1rem;
  background: var(--bg-color);
  border-top: 1px solid var(--border-color);
  font-size: 0.8rem;
  color: var(--text-muted);
}

.results-section {
  flex: 1;
  display: flex;
  flex-direction: column;
  margin: 0 0.75rem 0.75rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
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

.success-icon { color: #10b981; }
.error-icon { color: #ef4444; }

.results-info {
  font-size: 0.85rem;
  color: var(--text-muted);
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

.traceback {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid #fcc;
  color: #666;
}

.output-panel {
  flex: 1;
  overflow: auto;
  padding: 1rem;
}

.console-output, .variables-section, .dataframes-section {
  margin-bottom: 1.5rem;
}

.console-output h4, .variables-section h4, .dataframes-section h4 {
  margin: 0 0 0.75rem 0;
  font-size: 0.95rem;
  color: var(--text-color);
}

.console-output pre {
  background: #1e1e1e;
  color: #d4d4d4;
  padding: 1rem;
  border-radius: 6px;
  overflow-x: auto;
  margin: 0;
  font-size: 0.9rem;
}

.variables-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9rem;
}

.variables-table th, .variables-table td {
  text-align: left;
  padding: 0.5rem;
  border-bottom: 1px solid var(--border-color);
}

.variables-table th {
  background: var(--bg-color);
  font-weight: 600;
}

.var-name {
  font-family: monospace;
  color: #3572A5;
}

.var-value {
  font-family: monospace;
  word-break: break-all;
}

.dataframe-card {
  background: var(--bg-color);
  border-radius: 8px;
  margin-bottom: 1rem;
  overflow: hidden;
}

.df-header {
  display: flex;
  justify-content: space-between;
  padding: 0.75rem 1rem;
  background: #3572A5;
  color: white;
}

.df-name {
  font-weight: 600;
  font-family: monospace;
}

.df-info {
  font-size: 0.85rem;
  opacity: 0.9;
}

.df-error {
  padding: 1rem;
  color: #ef4444;
}

.df-preview {
  padding: 1rem;
  margin: 0;
  font-family: monospace;
  font-size: 0.85rem;
  overflow-x: auto;
  background: #1e1e1e;
  color: #d4d4d4;
}

.no-output {
  text-align: center;
  padding: 2rem;
  color: var(--text-muted);
}

/* Modal styles */
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
  margin: 0;
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

.template-card {
  background: var(--bg-color);
  border-radius: 8px;
  padding: 1rem;
  margin-bottom: 1rem;
  cursor: pointer;
  transition: all 0.2s ease;
}

.template-card:hover {
  border-color: #3572A5;
  box-shadow: 0 4px 12px rgba(53, 114, 165, 0.15);
}

.template-name {
  font-weight: 600;
  margin-bottom: 0.5rem;
}

.template-preview {
  font-family: monospace;
  font-size: 0.8rem;
  color: var(--text-muted);
  margin: 0;
  white-space: pre-wrap;
  max-height: 100px;
  overflow: hidden;
}
</style>
