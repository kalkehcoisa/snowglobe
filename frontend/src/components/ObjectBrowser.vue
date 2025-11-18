<template>
  <div class="object-browser">
    <div class="panel-header">
      <h2>{{ icon }} {{ title }}</h2>
      <button class="btn-secondary" @click="$emit('refresh')">
        🔄 Refresh
      </button>
    </div>

    <!-- Breadcrumb -->
    <div class="breadcrumb" v-if="breadcrumb.length">
      <span 
        v-for="(item, idx) in breadcrumb" 
        :key="idx"
        class="breadcrumb-item"
        @click="navigateTo(idx)"
      >
        {{ item }}
        <span v-if="idx < breadcrumb.length - 1" class="separator">/</span>
      </span>
    </div>

    <!-- Search/Filter -->
    <div class="search-bar">
      <input 
        v-model="searchQuery" 
        type="text" 
        :placeholder="`Search ${title.toLowerCase()}...`"
        class="search-input"
      />
    </div>

    <!-- Object List -->
    <div class="object-list">
      <div 
        v-for="obj in filteredObjects" 
        :key="obj.name"
        class="object-item"
        @click="selectObject(obj)"
        @dblclick="drillDown(obj)"
      >
        <span class="object-icon">{{ getObjectIcon(obj) }}</span>
        <div class="object-info">
          <span class="object-name">{{ obj.name }}</span>
          <span class="object-meta">{{ getObjectMeta(obj) }}</span>
        </div>
        <div class="object-actions">
          <button class="action-btn" @click.stop="copyName(obj.name)" title="Copy name">
            📋
          </button>
          <button class="action-btn" @click.stop="showQuery(obj)" title="Query">
            🔍
          </button>
        </div>
      </div>

      <div v-if="filteredObjects.length === 0" class="empty-state">
        <p v-if="searchQuery">No {{ title.toLowerCase() }} matching "{{ searchQuery }}"</p>
        <p v-else>No {{ title.toLowerCase() }} found</p>
      </div>
    </div>

    <!-- Object Details Panel -->
    <div class="details-panel" v-if="selectedObject">
      <div class="details-header">
        <h3>{{ selectedObject.name }}</h3>
        <button class="close-btn" @click="selectedObject = null">×</button>
      </div>
      <div class="details-content">
        <table class="details-table">
          <tr v-for="(value, key) in getObjectDetails(selectedObject)" :key="key">
            <td class="detail-key">{{ key }}</td>
            <td class="detail-value">{{ value }}</td>
          </tr>
        </table>
        
        <div class="detail-actions" v-if="type === 'tables'">
          <button class="btn-primary" @click="previewData(selectedObject)">
            👁️ Preview Data
          </button>
          <button class="btn-secondary" @click="describeObject(selectedObject)">
            📝 Describe
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'ObjectBrowser',
  props: {
    type: {
      type: String,
      required: true
    },
    title: {
      type: String,
      required: true
    },
    icon: {
      type: String,
      required: true
    },
    objects: {
      type: Array,
      default: () => []
    },
    canDrillDown: {
      type: Boolean,
      default: false
    }
  },
  data() {
    return {
      searchQuery: '',
      selectedObject: null,
      breadcrumb: []
    }
  },
  computed: {
    filteredObjects() {
      if (!this.searchQuery) return this.objects
      const query = this.searchQuery.toLowerCase()
      return this.objects.filter(obj => 
        obj.name.toLowerCase().includes(query)
      )
    }
  },
  methods: {
    getObjectIcon(obj) {
      const icons = {
        databases: '🗄️',
        schemas: '📁',
        tables: '📊',
        views: '👁️',
        stages: '📦',
        warehouses: '⚡'
      }
      return icons[this.type] || '📄'
    },
    
    getObjectMeta(obj) {
      if (obj.created_at) {
        return `Created: ${new Date(obj.created_at).toLocaleDateString()}`
      }
      if (obj.row_count !== undefined) {
        return `${obj.row_count} rows`
      }
      return ''
    },
    
    getObjectDetails(obj) {
      const details = {}
      for (const [key, value] of Object.entries(obj)) {
        if (key === 'columns') continue
        if (value !== null && value !== undefined) {
          details[this.formatKey(key)] = this.formatValue(key, value)
        }
      }
      return details
    },
    
    formatKey(key) {
      return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
    },
    
    formatValue(key, value) {
      if (key.includes('at') || key.includes('date')) {
        return new Date(value).toLocaleString()
      }
      return value
    },
    
    selectObject(obj) {
      this.selectedObject = obj
    },
    
    drillDown(obj) {
      if (this.canDrillDown) {
        this.$emit('drill-down', obj)
      }
    },
    
    navigateTo(idx) {
      this.$emit('navigate-breadcrumb', idx)
    },
    
    copyName(name) {
      navigator.clipboard.writeText(name)
      // Show toast notification
    },
    
    showQuery(obj) {
      this.$emit('show-query', obj)
    },
    
    previewData(obj) {
      this.$emit('preview-data', obj)
    },
    
    describeObject(obj) {
      this.$emit('describe-object', obj)
    }
  }
}
</script>

<style scoped>
.object-browser {
  display: flex;
  flex-direction: column;
  height: 100%;
  gap: 1rem;
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.panel-header h2 {
  font-size: 1.5rem;
  margin: 0;
}

.btn-secondary {
  padding: 0.5rem 1rem;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  background: var(--card-bg);
  cursor: pointer;
  font-size: 0.9rem;
}

.breadcrumb {
  display: flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.5rem 1rem;
  background: var(--card-bg);
  border-radius: 6px;
  font-size: 0.9rem;
}

.breadcrumb-item {
  cursor: pointer;
  color: var(--primary-color);
}

.breadcrumb-item:hover {
  text-decoration: underline;
}

.separator {
  color: var(--text-muted);
  margin: 0 0.25rem;
}

.search-bar {
  padding: 0.5rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
}

.search-input {
  width: 100%;
  padding: 0.5rem;
  border: none;
  background: transparent;
  font-size: 0.9rem;
}

.object-list {
  flex: 1;
  overflow-y: auto;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
}

.object-item {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--border-color);
  cursor: pointer;
  transition: background 0.2s ease;
}

.object-item:hover {
  background: var(--bg-color);
}

.object-item:last-child {
  border-bottom: none;
}

.object-icon {
  font-size: 1.25rem;
}

.object-info {
  flex: 1;
  display: flex;
  flex-direction: column;
}

.object-name {
  font-weight: 500;
}

.object-meta {
  font-size: 0.8rem;
  color: var(--text-muted);
}

.object-actions {
  display: flex;
  gap: 0.25rem;
  opacity: 0;
  transition: opacity 0.2s ease;
}

.object-item:hover .object-actions {
  opacity: 1;
}

.action-btn {
  padding: 0.375rem;
  background: none;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.85rem;
}

.action-btn:hover {
  background: var(--bg-color);
}

.empty-state {
  padding: 2rem;
  text-align: center;
  color: var(--text-muted);
}

.details-panel {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  max-height: 300px;
  overflow-y: auto;
}

.details-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--border-color);
  background: var(--bg-color);
}

.details-header h3 {
  margin: 0;
  font-size: 1rem;
}

.close-btn {
  background: none;
  border: none;
  font-size: 1.25rem;
  cursor: pointer;
  color: var(--text-muted);
}

.details-content {
  padding: 1rem;
}

.details-table {
  width: 100%;
  font-size: 0.85rem;
}

.details-table td {
  padding: 0.375rem 0;
}

.detail-key {
  color: var(--text-muted);
  width: 40%;
}

.detail-value {
  font-weight: 500;
}

.detail-actions {
  display: flex;
  gap: 0.5rem;
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--border-color);
}

.btn-primary {
  padding: 0.5rem 1rem;
  background: var(--primary-color);
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
}
</style>
