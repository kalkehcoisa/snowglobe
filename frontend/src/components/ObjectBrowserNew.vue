<template>
  <div class="object-browser">
    <div class="browser-header">
      <h2>🗄️ Object Browser</h2>
      <div class="header-actions">
        <input 
          v-model="searchQuery"
          type="text"
          placeholder="Search objects..."
          class="search-input"
          @input="debouncedSearch"
        />
        <button class="btn-icon" @click="refreshAll" :disabled="isLoading">
          🔄
        </button>
      </div>
    </div>

    <!-- Search Results -->
    <div v-if="searchQuery && searchResults.length > 0" class="search-results">
      <div class="search-header">
        <span>Search Results ({{ searchResults.length }})</span>
        <button class="btn-link" @click="clearSearch">Clear</button>
      </div>
      <div 
        v-for="result in searchResults" 
        :key="result.full_name"
        class="search-result-item"
        @click="selectSearchResult(result)"
      >
        <span :class="['result-icon', result.type.toLowerCase()]">
          {{ getTypeIcon(result.type) }}
        </span>
        <div class="result-info">
          <span class="result-name">{{ result.name }}</span>
          <span class="result-path">{{ result.full_name }}</span>
        </div>
        <span class="result-type-badge">{{ result.type }}</span>
      </div>
    </div>

    <!-- Stacked Filters -->
    <div class="stacked-filters" v-else>
      <!-- Database Filter -->
      <div class="filter-section">
        <div class="filter-header" @click="toggleSection('databases')">
          <span class="filter-icon">🗄️</span>
          <span class="filter-title">Database</span>
          <span v-if="selectedDatabase" class="selected-value">{{ selectedDatabase }}</span>
          <span class="expand-icon">{{ expandedSections.databases ? '▼' : '▶' }}</span>
        </div>
        <div v-show="expandedSections.databases" class="filter-options">
          <div 
            v-for="db in databases" 
            :key="db.name"
            :class="['filter-option', { selected: selectedDatabase === db.name }]"
            @click="selectDatabase(db.name)"
          >
            <span class="option-icon">📀</span>
            <span class="option-name">{{ db.name }}</span>
            <span v-if="selectedDatabase === db.name" class="check-icon">✓</span>
          </div>
          <div v-if="databases.length === 0" class="empty-message">
            No databases found
          </div>
        </div>
      </div>

      <!-- Schema Filter (visible when database selected) -->
      <div class="filter-section" v-if="selectedDatabase">
        <div class="filter-header" @click="toggleSection('schemas')">
          <span class="filter-icon">📁</span>
          <span class="filter-title">Schema</span>
          <span v-if="selectedSchema" class="selected-value">{{ selectedSchema }}</span>
          <span class="expand-icon">{{ expandedSections.schemas ? '▼' : '▶' }}</span>
        </div>
        <div v-show="expandedSections.schemas" class="filter-options">
          <div 
            class="filter-option all-option"
            :class="{ selected: !selectedSchema }"
            @click="selectSchema(null)"
          >
            <span class="option-icon">📋</span>
            <span class="option-name">All Schemas</span>
          </div>
          <div 
            v-for="schema in schemas" 
            :key="schema.name"
            :class="['filter-option', { selected: selectedSchema === schema.name }]"
            @click="selectSchema(schema.name)"
          >
            <span class="option-icon">📂</span>
            <span class="option-name">{{ schema.name }}</span>
            <span v-if="selectedSchema === schema.name" class="check-icon">✓</span>
          </div>
          <div v-if="loadingSchemas" class="loading-message">
            Loading schemas...
          </div>
        </div>
      </div>

      <!-- Object Type Filter -->
      <div class="filter-section" v-if="selectedDatabase">
        <div class="filter-header" @click="toggleSection('types')">
          <span class="filter-icon">📊</span>
          <span class="filter-title">Object Type</span>
          <span v-if="selectedType" class="selected-value">{{ selectedType }}</span>
          <span class="expand-icon">{{ expandedSections.types ? '▼' : '▶' }}</span>
        </div>
        <div v-show="expandedSections.types" class="filter-options">
          <div 
            class="filter-option all-option"
            :class="{ selected: !selectedType }"
            @click="selectType(null)"
          >
            <span class="option-icon">🔷</span>
            <span class="option-name">All Types</span>
          </div>
          <div 
            class="filter-option"
            :class="{ selected: selectedType === 'TABLE' }"
            @click="selectType('TABLE')"
          >
            <span class="option-icon">📊</span>
            <span class="option-name">Tables</span>
            <span class="count-badge">{{ tableCount }}</span>
          </div>
          <div 
            class="filter-option"
            :class="{ selected: selectedType === 'VIEW' }"
            @click="selectType('VIEW')"
          >
            <span class="option-icon">👁️</span>
            <span class="option-name">Views</span>
            <span class="count-badge">{{ viewCount }}</span>
          </div>
        </div>
      </div>
    </div>

    <!-- Objects List -->
    <div class="objects-list" v-if="selectedDatabase && !searchQuery">
      <div class="list-header">
        <span class="list-title">
          {{ filteredObjects.length }} {{ selectedType || 'Object' }}{{ filteredObjects.length !== 1 ? 's' : '' }}
        </span>
        <div class="list-actions">
          <button 
            :class="['view-btn', { active: viewMode === 'list' }]" 
            @click="viewMode = 'list'"
          >≡</button>
          <button 
            :class="['view-btn', { active: viewMode === 'grid' }]" 
            @click="viewMode = 'grid'"
          >⊞</button>
        </div>
      </div>

      <div v-if="loadingObjects" class="loading-state">
        <span class="spinner">⏳</span>
        Loading objects...
      </div>

      <div v-else-if="filteredObjects.length === 0" class="empty-state">
        <p>No {{ selectedType?.toLowerCase() || 'objects' }} found</p>
      </div>

      <div v-else :class="['objects-container', viewMode]">
        <div 
          v-for="obj in filteredObjects" 
          :key="obj.name"
          :class="['object-card', obj.type.toLowerCase()]"
          @click="selectObject(obj)"
        >
          <div class="object-header">
            <span class="object-icon">{{ getTypeIcon(obj.type) }}</span>
            <span class="object-name">{{ obj.name }}</span>
          </div>
          <div class="object-meta">
            <span class="object-type">{{ obj.type }}</span>
            <span v-if="obj.row_count !== undefined" class="object-rows">
              {{ formatNumber(obj.row_count) }} rows
            </span>
          </div>
          <div class="object-actions">
            <button @click.stop="previewData(obj)" title="Preview Data">👀</button>
            <button @click.stop="copyName(obj)" title="Copy Name">📋</button>
            <button @click.stop="openInWorksheet(obj)" title="Open in Worksheet">📝</button>
          </div>
        </div>
      </div>
    </div>

    <!-- Object Details Panel -->
    <div v-if="selectedObject" class="details-panel">
      <div class="details-header">
        <h3>
          <span class="details-icon">{{ getTypeIcon(selectedObject.type) }}</span>
          {{ selectedObject.name }}
        </h3>
        <button class="close-btn" @click="selectedObject = null">×</button>
      </div>
      <div class="details-content">
        <div class="detail-row">
          <span class="detail-label">Full Name:</span>
          <code class="detail-value">
            {{ selectedDatabase }}.{{ selectedSchema || 'PUBLIC' }}.{{ selectedObject.name }}
          </code>
        </div>
        <div class="detail-row">
          <span class="detail-label">Type:</span>
          <span class="detail-value">{{ selectedObject.type }}</span>
        </div>
        <div class="detail-row" v-if="selectedObject.created_at">
          <span class="detail-label">Created:</span>
          <span class="detail-value">{{ formatDate(selectedObject.created_at) }}</span>
        </div>
        <div class="detail-row" v-if="selectedObject.row_count !== undefined">
          <span class="detail-label">Row Count:</span>
          <span class="detail-value">{{ formatNumber(selectedObject.row_count) }}</span>
        </div>
        
        <!-- Columns -->
        <div v-if="selectedObject.columns && selectedObject.columns.length > 0" class="columns-section">
          <h4>Columns ({{ selectedObject.columns.length }})</h4>
          <table class="columns-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Name</th>
                <th>Type</th>
                <th>Nullable</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(col, idx) in selectedObject.columns" :key="col.name">
                <td class="col-num">{{ idx + 1 }}</td>
                <td class="col-name">{{ col.name }}</td>
                <td class="col-type">{{ col.type }}</td>
                <td class="col-nullable">{{ col.nullable === 'Y' ? 'Yes' : 'No' }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- View Definition -->
        <div v-if="selectedObject.definition" class="definition-section">
          <h4>View Definition</h4>
          <pre class="definition-code">{{ selectedObject.definition }}</pre>
        </div>

        <div class="details-actions">
          <button class="btn-primary" @click="openInWorksheet(selectedObject)">
            📝 Open in Worksheet
          </button>
          <button class="btn-secondary" @click="previewData(selectedObject)">
            👀 Preview Data
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'ObjectBrowserNew',
  emits: ['open-worksheet', 'preview-data'],
  data() {
    return {
      databases: [],
      schemas: [],
      objects: [],
      
      selectedDatabase: null,
      selectedSchema: null,
      selectedType: null,
      selectedObject: null,
      
      searchQuery: '',
      searchResults: [],
      searchTimeout: null,
      
      expandedSections: {
        databases: true,
        schemas: true,
        types: false
      },
      
      viewMode: 'list',
      
      isLoading: false,
      loadingSchemas: false,
      loadingObjects: false
    }
  },
  computed: {
    filteredObjects() {
      if (!this.selectedType) {
        return this.objects
      }
      return this.objects.filter(obj => obj.type === this.selectedType)
    },
    tableCount() {
      return this.objects.filter(obj => obj.type === 'TABLE').length
    },
    viewCount() {
      return this.objects.filter(obj => obj.type === 'VIEW').length
    }
  },
  methods: {
    async fetchDatabases() {
      try {
        const response = await axios.get('/api/browser/databases')
        this.databases = response.data.databases
      } catch (error) {
        console.error('Failed to fetch databases:', error)
      }
    },
    
    async selectDatabase(dbName) {
      if (this.selectedDatabase === dbName) {
        return
      }
      
      this.selectedDatabase = dbName
      this.selectedSchema = null
      this.selectedObject = null
      this.schemas = []
      this.objects = []
      
      await this.fetchSchemas(dbName)
      await this.fetchObjects()
    },
    
    async fetchSchemas(database) {
      this.loadingSchemas = true
      try {
        const response = await axios.get(`/api/browser/databases/${database}/schemas`)
        this.schemas = response.data.schemas
      } catch (error) {
        console.error('Failed to fetch schemas:', error)
      } finally {
        this.loadingSchemas = false
      }
    },
    
    async selectSchema(schemaName) {
      this.selectedSchema = schemaName
      this.selectedObject = null
      await this.fetchObjects()
    },
    
    selectType(type) {
      this.selectedType = type
    },
    
    async fetchObjects() {
      if (!this.selectedDatabase) return
      
      this.loadingObjects = true
      try {
        const schema = this.selectedSchema || 'PUBLIC'
        const response = await axios.get(
          `/api/browser/databases/${this.selectedDatabase}/schemas/${schema}/objects`
        )
        this.objects = response.data.objects
      } catch (error) {
        console.error('Failed to fetch objects:', error)
        this.objects = []
      } finally {
        this.loadingObjects = false
      }
    },
    
    selectObject(obj) {
      this.selectedObject = obj
    },
    
    toggleSection(section) {
      this.expandedSections[section] = !this.expandedSections[section]
    },
    
    debouncedSearch() {
      if (this.searchTimeout) {
        clearTimeout(this.searchTimeout)
      }
      
      this.searchTimeout = setTimeout(() => {
        this.performSearch()
      }, 300)
    },
    
    async performSearch() {
      if (!this.searchQuery || this.searchQuery.length < 2) {
        this.searchResults = []
        return
      }
      
      try {
        const params = new URLSearchParams({ q: this.searchQuery })
        if (this.selectedDatabase) {
          params.append('database', this.selectedDatabase)
        }
        
        const response = await axios.get(`/api/browser/search?${params}`)
        this.searchResults = response.data.results
      } catch (error) {
        console.error('Search failed:', error)
        this.searchResults = []
      }
    },
    
    clearSearch() {
      this.searchQuery = ''
      this.searchResults = []
    },
    
    selectSearchResult(result) {
      // Navigate to the selected object
      if (result.type === 'DATABASE') {
        this.selectDatabase(result.name)
      } else if (result.type === 'SCHEMA') {
        this.selectDatabase(result.database)
        this.$nextTick(() => {
          this.selectSchema(result.name)
        })
      } else {
        this.selectDatabase(result.database)
        this.$nextTick(() => {
          this.selectSchema(result.schema)
          this.$nextTick(() => {
            const obj = this.objects.find(o => o.name === result.name)
            if (obj) {
              this.selectObject(obj)
            }
          })
        })
      }
      
      this.clearSearch()
    },
    
    async refreshAll() {
      this.isLoading = true
      await this.fetchDatabases()
      if (this.selectedDatabase) {
        await this.fetchSchemas(this.selectedDatabase)
        await this.fetchObjects()
      }
      this.isLoading = false
    },
    
    getTypeIcon(type) {
      const icons = {
        'DATABASE': '🗄️',
        'SCHEMA': '📁',
        'TABLE': '📊',
        'VIEW': '👁️'
      }
      return icons[type] || '📄'
    },
    
    formatNumber(num) {
      if (num === undefined || num === null) return '-'
      return num.toLocaleString()
    },
    
    formatDate(dateStr) {
      if (!dateStr) return '-'
      return new Date(dateStr).toLocaleString()
    },
    
    copyName(obj) {
      const fullName = `${this.selectedDatabase}.${this.selectedSchema || 'PUBLIC'}.${obj.name}`
      navigator.clipboard.writeText(fullName)
    },
    
    previewData(obj) {
      const sql = `SELECT * FROM ${this.selectedDatabase}.${this.selectedSchema || 'PUBLIC'}.${obj.name} LIMIT 100;`
      this.$emit('preview-data', { object: obj, sql })
    },
    
    openInWorksheet(obj) {
      const sql = `-- ${obj.type}: ${obj.name}\nSELECT * FROM ${this.selectedDatabase}.${this.selectedSchema || 'PUBLIC'}.${obj.name} LIMIT 100;`
      this.$emit('open-worksheet', { sql, object: obj })
    }
  },
  async mounted() {
    await this.fetchDatabases()
    
    // Auto-select first database if available
    if (this.databases.length > 0) {
      await this.selectDatabase(this.databases[0].name)
    }
  }
}
</script>

<style scoped>
.object-browser {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--card-bg);
  border-radius: 12px;
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.browser-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem 1.25rem;
  border-bottom: 1px solid var(--border-color);
  background: var(--bg-color);
}

.browser-header h2 {
  font-size: 1.1rem;
  font-weight: 600;
  margin: 0;
}

.header-actions {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.search-input {
  padding: 0.5rem 0.75rem;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  font-size: 0.9rem;
  width: 200px;
}

.btn-icon {
  padding: 0.5rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  cursor: pointer;
}

.btn-icon:hover {
  background: var(--bg-color);
}

/* Search Results */
.search-results {
  max-height: 300px;
  overflow-y: auto;
  border-bottom: 1px solid var(--border-color);
}

.search-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem 1rem;
  background: var(--bg-color);
  font-size: 0.85rem;
  font-weight: 500;
}

.btn-link {
  background: none;
  border: none;
  color: var(--primary-color);
  cursor: pointer;
  font-size: 0.85rem;
}

.search-result-item {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.75rem 1rem;
  cursor: pointer;
  border-bottom: 1px solid var(--border-color);
}

.search-result-item:hover {
  background: var(--bg-color);
}

.result-icon {
  font-size: 1.25rem;
}

.result-info {
  flex: 1;
  min-width: 0;
}

.result-name {
  font-weight: 500;
  display: block;
}

.result-path {
  font-size: 0.8rem;
  color: var(--text-muted);
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
}

.result-type-badge {
  font-size: 0.75rem;
  padding: 0.2rem 0.5rem;
  border-radius: 4px;
  background: var(--bg-color);
  color: var(--text-muted);
}

/* Stacked Filters */
.stacked-filters {
  border-bottom: 1px solid var(--border-color);
}

.filter-section {
  border-bottom: 1px solid var(--border-color);
}

.filter-section:last-child {
  border-bottom: none;
}

.filter-header {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.875rem 1rem;
  cursor: pointer;
  transition: background 0.2s;
}

.filter-header:hover {
  background: var(--bg-color);
}

.filter-icon {
  font-size: 1.1rem;
}

.filter-title {
  font-weight: 500;
  flex: 1;
}

.selected-value {
  font-size: 0.85rem;
  color: var(--primary-color);
  font-weight: 500;
  background: rgba(41, 181, 232, 0.1);
  padding: 0.2rem 0.5rem;
  border-radius: 4px;
}

.expand-icon {
  font-size: 0.75rem;
  color: var(--text-muted);
}

.filter-options {
  padding: 0.5rem;
  background: var(--bg-color);
  max-height: 200px;
  overflow-y: auto;
}

.filter-option {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.625rem 0.75rem;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.15s;
}

.filter-option:hover {
  background: var(--card-bg);
}

.filter-option.selected {
  background: rgba(41, 181, 232, 0.15);
  color: var(--primary-color);
}

.filter-option.all-option {
  font-style: italic;
  opacity: 0.85;
}

.option-icon {
  font-size: 1rem;
}

.option-name {
  flex: 1;
  font-size: 0.9rem;
}

.check-icon {
  color: var(--primary-color);
  font-weight: bold;
}

.count-badge {
  font-size: 0.75rem;
  background: var(--card-bg);
  padding: 0.15rem 0.4rem;
  border-radius: 4px;
  color: var(--text-muted);
}

.loading-message,
.empty-message {
  padding: 1rem;
  text-align: center;
  color: var(--text-muted);
  font-size: 0.9rem;
}

/* Objects List */
.objects-list {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.list-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--border-color);
}

.list-title {
  font-size: 0.9rem;
  font-weight: 500;
}

.list-actions {
  display: flex;
  gap: 0.25rem;
}

.view-btn {
  padding: 0.375rem 0.5rem;
  background: var(--bg-color);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.9rem;
}

.view-btn.active {
  background: var(--primary-color);
  color: white;
  border-color: var(--primary-color);
}

.loading-state,
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 2rem;
  color: var(--text-muted);
}

.spinner {
  font-size: 1.5rem;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.objects-container {
  flex: 1;
  overflow-y: auto;
  padding: 0.75rem;
}

.objects-container.grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 0.75rem;
}

.objects-container.list .object-card {
  margin-bottom: 0.5rem;
}

.object-card {
  background: var(--bg-color);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 1rem;
  cursor: pointer;
  transition: all 0.2s;
}

.object-card:hover {
  border-color: var(--primary-color);
  box-shadow: 0 2px 8px rgba(41, 181, 232, 0.15);
}

.object-card.table {
  border-left: 3px solid #3b82f6;
}

.object-card.view {
  border-left: 3px solid #8b5cf6;
}

.object-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.5rem;
}

.object-icon {
  font-size: 1.1rem;
}

.object-name {
  font-weight: 600;
  font-size: 0.95rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.object-meta {
  display: flex;
  justify-content: space-between;
  font-size: 0.8rem;
  color: var(--text-muted);
  margin-bottom: 0.75rem;
}

.object-type {
  background: var(--card-bg);
  padding: 0.15rem 0.4rem;
  border-radius: 4px;
}

.object-actions {
  display: flex;
  gap: 0.5rem;
}

.object-actions button {
  flex: 1;
  padding: 0.375rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.85rem;
}

.object-actions button:hover {
  background: var(--primary-color);
  color: white;
  border-color: var(--primary-color);
}

/* Details Panel */
.details-panel {
  position: absolute;
  top: 0;
  right: 0;
  width: 400px;
  height: 100%;
  background: var(--card-bg);
  border-left: 1px solid var(--border-color);
  box-shadow: -4px 0 20px rgba(0, 0, 0, 0.1);
  z-index: 10;
  display: flex;
  flex-direction: column;
}

.details-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem 1.25rem;
  border-bottom: 1px solid var(--border-color);
}

.details-header h3 {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 1rem;
  margin: 0;
}

.details-icon {
  font-size: 1.25rem;
}

.close-btn {
  background: none;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  color: var(--text-muted);
  padding: 0;
  line-height: 1;
}

.details-content {
  flex: 1;
  overflow-y: auto;
  padding: 1.25rem;
}

.detail-row {
  display: flex;
  margin-bottom: 1rem;
}

.detail-label {
  width: 100px;
  font-size: 0.85rem;
  color: var(--text-muted);
  flex-shrink: 0;
}

.detail-value {
  font-size: 0.9rem;
}

.detail-value code {
  background: var(--bg-color);
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
  font-family: monospace;
  font-size: 0.85rem;
}

.columns-section,
.definition-section {
  margin-top: 1.5rem;
}

.columns-section h4,
.definition-section h4 {
  font-size: 0.95rem;
  margin: 0 0 1rem 0;
}

.columns-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}

.columns-table th,
.columns-table td {
  padding: 0.5rem;
  text-align: left;
  border-bottom: 1px solid var(--border-color);
}

.columns-table th {
  background: var(--bg-color);
  font-weight: 500;
}

.col-num {
  width: 30px;
  color: var(--text-muted);
}

.col-type {
  font-family: monospace;
  font-size: 0.8rem;
}

.definition-code {
  background: #1e1e1e;
  color: #d4d4d4;
  padding: 1rem;
  border-radius: 6px;
  font-family: monospace;
  font-size: 0.85rem;
  overflow-x: auto;
  white-space: pre-wrap;
}

.details-actions {
  display: flex;
  gap: 0.75rem;
  margin-top: 1.5rem;
  padding-top: 1.5rem;
  border-top: 1px solid var(--border-color);
}

.btn-primary,
.btn-secondary {
  flex: 1;
  padding: 0.625rem 1rem;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.9rem;
  font-weight: 500;
}

.btn-primary {
  background: var(--primary-color);
  color: white;
}

.btn-secondary {
  background: var(--bg-color);
  border: 1px solid var(--border-color);
}
</style>
