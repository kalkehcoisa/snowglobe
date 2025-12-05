<template>
  <div class="object-manager">
    <div class="panel-header">
      <h2 class="section-title">🛠️ Object Manager</h2>
      <p class="description">Create, modify, and manage Snowflake objects</p>
    </div>

    <!-- Object Type Tabs -->
    <div class="object-tabs">
      <button 
        v-for="type in objectTypes" 
        :key="type.id"
        :class="['tab-btn', { active: selectedType === type.id }]"
        @click="selectedType = type.id"
      >
        <span class="tab-icon">{{ type.icon }}</span>
        {{ type.name }}
      </button>
    </div>

    <!-- Action Buttons -->
    <div class="action-bar">
      <button class="btn-primary" @click="showCreateModal = true">
        ➕ Create {{ currentTypeName }}
      </button>
      <button class="btn-secondary" @click="refreshObjects">
        🔄 Refresh
      </button>
    </div>

    <!-- Objects List -->
    <div class="objects-list">
      <div v-if="isLoading" class="loading">Loading...</div>
      <div v-else-if="objects.length === 0" class="empty-state">
        No {{ currentTypeName.toLowerCase() }}s found. Create one to get started.
      </div>
      <div v-else class="objects-grid">
        <div v-for="obj in objects" :key="obj.name" class="object-card">
          <div class="card-header">
            <span class="obj-icon">{{ getObjectIcon(obj) }}</span>
            <span class="obj-name">{{ obj.name }}</span>
          </div>
          <div class="card-body">
            <p v-if="obj.type">Type: {{ obj.type }}</p>
            <p v-if="obj.created_at">Created: {{ formatDate(obj.created_at) }}</p>
            <p v-if="obj.row_count !== undefined">Rows: {{ obj.row_count }}</p>
          </div>
          <div class="card-actions">
            <button class="action-btn" @click="editObject(obj)" title="Edit">
              ✏️
            </button>
            <button class="action-btn" @click="viewDDL(obj)" title="View DDL">
              📝
            </button>
            <button class="action-btn danger" @click="deleteObject(obj)" title="Delete">
              🗑️
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Create Modal -->
    <div v-if="showCreateModal" class="modal-overlay" @click.self="showCreateModal = false">
      <div class="modal-content">
        <div class="modal-header">
          <h3>Create {{ currentTypeName }}</h3>
          <button class="close-btn" @click="showCreateModal = false">×</button>
        </div>
        <div class="modal-body">
          <!-- Database Create -->
          <div v-if="selectedType === 'database'" class="form-section">
            <div class="form-group">
              <label>Database Name *</label>
              <input v-model="createForm.name" type="text" placeholder="MY_DATABASE">
            </div>
            <div class="form-group">
              <label>Comment</label>
              <input v-model="createForm.comment" type="text" placeholder="Optional description">
            </div>
            <div class="form-group checkbox">
              <label>
                <input type="checkbox" v-model="createForm.ifNotExists">
                IF NOT EXISTS
              </label>
            </div>
            <div class="form-group checkbox">
              <label>
                <input type="checkbox" v-model="createForm.transient">
                Transient
              </label>
            </div>
          </div>

          <!-- Schema Create -->
          <div v-if="selectedType === 'schema'" class="form-section">
            <div class="form-group">
              <label>Database *</label>
              <select v-model="createForm.database">
                <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
              </select>
            </div>
            <div class="form-group">
              <label>Schema Name *</label>
              <input v-model="createForm.name" type="text" placeholder="MY_SCHEMA">
            </div>
            <div class="form-group">
              <label>Comment</label>
              <input v-model="createForm.comment" type="text" placeholder="Optional description">
            </div>
            <div class="form-group checkbox">
              <label>
                <input type="checkbox" v-model="createForm.ifNotExists">
                IF NOT EXISTS
              </label>
            </div>
          </div>

          <!-- Table Create -->
          <div v-if="selectedType === 'table'" class="form-section">
            <div class="form-row">
              <div class="form-group">
                <label>Database *</label>
                <select v-model="createForm.database" @change="onDatabaseChange">
                  <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
                </select>
              </div>
              <div class="form-group">
                <label>Schema *</label>
                <select v-model="createForm.schema">
                  <option v-for="sch in schemas" :key="sch.name" :value="sch.name">{{ sch.name }}</option>
                </select>
              </div>
            </div>
            <div class="form-group">
              <label>Table Name *</label>
              <input v-model="createForm.name" type="text" placeholder="MY_TABLE">
            </div>
            
            <div class="columns-editor">
              <h4>Columns</h4>
              <div v-for="(col, idx) in createForm.columns" :key="idx" class="column-row">
                <input v-model="col.name" placeholder="Column name" class="col-name">
                <select v-model="col.type" class="col-type">
                  <option v-for="t in dataTypes" :key="t" :value="t">{{ t }}</option>
                </select>
                <label class="col-nullable">
                  <input type="checkbox" v-model="col.nullable">
                  Nullable
                </label>
                <label class="col-pk">
                  <input type="checkbox" v-model="col.primaryKey">
                  PK
                </label>
                <button class="remove-col-btn" @click="removeColumn(idx)">×</button>
              </div>
              <button class="add-col-btn" @click="addColumn">+ Add Column</button>
            </div>

            <div class="form-group">
              <label>Comment</label>
              <input v-model="createForm.comment" type="text" placeholder="Optional description">
            </div>
            <div class="form-group checkbox">
              <label>
                <input type="checkbox" v-model="createForm.ifNotExists">
                IF NOT EXISTS
              </label>
            </div>
          </div>

          <!-- View Create -->
          <div v-if="selectedType === 'view'" class="form-section">
            <div class="form-row">
              <div class="form-group">
                <label>Database *</label>
                <select v-model="createForm.database" @change="onDatabaseChange">
                  <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
                </select>
              </div>
              <div class="form-group">
                <label>Schema *</label>
                <select v-model="createForm.schema">
                  <option v-for="sch in schemas" :key="sch.name" :value="sch.name">{{ sch.name }}</option>
                </select>
              </div>
            </div>
            <div class="form-group">
              <label>View Name *</label>
              <input v-model="createForm.name" type="text" placeholder="MY_VIEW">
            </div>
            <div class="form-group">
              <label>Definition (SELECT statement) *</label>
              <textarea v-model="createForm.definition" class="sql-input" placeholder="SELECT * FROM my_table"></textarea>
            </div>
            <div class="form-group checkbox">
              <label>
                <input type="checkbox" v-model="createForm.secure">
                Secure View
              </label>
            </div>
          </div>

          <!-- Stage Create -->
          <div v-if="selectedType === 'stage'" class="form-section">
            <div class="form-row">
              <div class="form-group">
                <label>Database *</label>
                <select v-model="createForm.database" @change="onDatabaseChange">
                  <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
                </select>
              </div>
              <div class="form-group">
                <label>Schema *</label>
                <select v-model="createForm.schema">
                  <option v-for="sch in schemas" :key="sch.name" :value="sch.name">{{ sch.name }}</option>
                </select>
              </div>
            </div>
            <div class="form-group">
              <label>Stage Name *</label>
              <input v-model="createForm.name" type="text" placeholder="MY_STAGE">
            </div>
            <div class="form-group">
              <label>URL (for external stage, optional)</label>
              <input v-model="createForm.url" type="text" placeholder="s3://bucket/path/">
            </div>
            <div class="form-group">
              <label>Comment</label>
              <input v-model="createForm.comment" type="text" placeholder="Optional description">
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn-secondary" @click="showCreateModal = false">Cancel</button>
          <button class="btn-primary" @click="executeCreate" :disabled="!canCreate">
            Create
          </button>
        </div>
      </div>
    </div>

    <!-- DDL Modal -->
    <div v-if="showDDLModal" class="modal-overlay" @click.self="showDDLModal = false">
      <div class="modal-content">
        <div class="modal-header">
          <h3>DDL: {{ ddlObject?.name }}</h3>
          <button class="close-btn" @click="showDDLModal = false">×</button>
        </div>
        <div class="modal-body">
          <pre class="ddl-content">{{ ddlContent }}</pre>
        </div>
        <div class="modal-footer">
          <button class="btn-secondary" @click="copyDDL">📋 Copy</button>
          <button class="btn-secondary" @click="showDDLModal = false">Close</button>
        </div>
      </div>
    </div>

    <!-- Confirm Delete Modal -->
    <div v-if="showDeleteModal" class="modal-overlay" @click.self="showDeleteModal = false">
      <div class="modal-content modal-small">
        <div class="modal-header">
          <h3>Confirm Delete</h3>
          <button class="close-btn" @click="showDeleteModal = false">×</button>
        </div>
        <div class="modal-body">
          <p>Are you sure you want to delete <strong>{{ deleteTarget?.name }}</strong>?</p>
          <p class="warning">This action cannot be undone (unless using UNDROP within retention period).</p>
        </div>
        <div class="modal-footer">
          <button class="btn-secondary" @click="showDeleteModal = false">Cancel</button>
          <button class="btn-danger" @click="confirmDelete">Delete</button>
        </div>
      </div>
    </div>

    <!-- Result Messages -->
    <div v-if="message" :class="['message', message.type]">
      {{ message.text }}
      <button class="close-msg" @click="message = null">×</button>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  name: 'ObjectManager',
  data() {
    return {
      selectedType: 'database',
      objectTypes: [
        { id: 'database', name: 'Databases', icon: '🗄️' },
        { id: 'schema', name: 'Schemas', icon: '📁' },
        { id: 'table', name: 'Tables', icon: '📊' },
        { id: 'view', name: 'Views', icon: '👁️' },
        { id: 'stage', name: 'Stages', icon: '📦' }
      ],
      dataTypes: [
        'VARCHAR', 'VARCHAR(255)', 'VARCHAR(1000)', 'TEXT',
        'INTEGER', 'BIGINT', 'SMALLINT',
        'FLOAT', 'DOUBLE', 'DECIMAL(18,2)',
        'BOOLEAN',
        'DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ',
        'VARIANT', 'OBJECT', 'ARRAY'
      ],
      objects: [],
      databases: [],
      schemas: [],
      isLoading: false,
      
      showCreateModal: false,
      showDDLModal: false,
      showDeleteModal: false,
      
      createForm: {
        name: '',
        database: '',
        schema: '',
        comment: '',
        ifNotExists: true,
        transient: false,
        columns: [{ name: '', type: 'VARCHAR', nullable: true, primaryKey: false }],
        definition: '',
        secure: false,
        url: ''
      },
      
      ddlObject: null,
      ddlContent: '',
      deleteTarget: null,
      message: null
    }
  },
  computed: {
    currentTypeName() {
      const type = this.objectTypes.find(t => t.id === this.selectedType)
      return type ? type.name.replace(/s$/, '') : 'Object'
    },
    canCreate() {
      if (!this.createForm.name) return false
      
      switch (this.selectedType) {
        case 'database':
          return true
        case 'schema':
          return !!this.createForm.database
        case 'table':
          return !!this.createForm.database && !!this.createForm.schema && 
                 this.createForm.columns.some(c => c.name)
        case 'view':
          return !!this.createForm.database && !!this.createForm.schema && 
                 !!this.createForm.definition
        case 'stage':
          return !!this.createForm.database && !!this.createForm.schema
        default:
          return false
      }
    }
  },
  watch: {
    selectedType: {
      handler() {
        this.refreshObjects()
        this.resetForm()
      },
      immediate: true
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
      if (!this.createForm.database) {
        this.schemas = []
        return
      }
      
      try {
        const response = await axios.get(`/api/databases/${this.createForm.database}/schemas`)
        this.schemas = response.data.schemas
      } catch (error) {
        console.error('Failed to fetch schemas:', error)
        this.schemas = []
      }
    },
    
    async refreshObjects() {
      this.isLoading = true
      
      try {
        switch (this.selectedType) {
          case 'database':
            const dbResponse = await axios.get('/api/databases')
            this.objects = dbResponse.data.databases
            break
            
          case 'schema':
            // Get schemas from all databases
            this.objects = []
            for (const db of this.databases) {
              try {
                const schResponse = await axios.get(`/api/databases/${db.name}/schemas`)
                const schemasWithDb = schResponse.data.schemas.map(s => ({
                  ...s,
                  database: db.name,
                  fullName: `${db.name}.${s.name}`
                }))
                this.objects.push(...schemasWithDb)
              } catch (e) {
                console.error(`Failed to get schemas for ${db.name}:`, e)
              }
            }
            break
            
          case 'table':
          case 'view':
            this.objects = []
            for (const db of this.databases) {
              try {
                const schResponse = await axios.get(`/api/databases/${db.name}/schemas`)
                for (const sch of schResponse.data.schemas) {
                  if (sch.name === 'INFORMATION_SCHEMA') continue
                  try {
                    const objResponse = await axios.get(
                      `/api/browser/databases/${db.name}/schemas/${sch.name}/objects`,
                      { params: { object_type: this.selectedType.toUpperCase() } }
                    )
                    const objects = objResponse.data.objects.map(o => ({
                      ...o,
                      database: db.name,
                      schema: sch.name,
                      fullName: `${db.name}.${sch.name}.${o.name}`
                    }))
                    this.objects.push(...objects)
                  } catch (e) {
                    // Ignore errors for individual schemas
                  }
                }
              } catch (e) {
                console.error(`Failed to get schemas for ${db.name}:`, e)
              }
            }
            break
            
          case 'stage':
            // Stages would need a separate API endpoint
            this.objects = []
            break
        }
      } catch (error) {
        console.error('Failed to refresh objects:', error)
        this.showMessage('Failed to load objects', 'error')
      } finally {
        this.isLoading = false
      }
    },
    
    resetForm() {
      this.createForm = {
        name: '',
        database: this.databases[0]?.name || '',
        schema: '',
        comment: '',
        ifNotExists: true,
        transient: false,
        columns: [{ name: '', type: 'VARCHAR', nullable: true, primaryKey: false }],
        definition: '',
        secure: false,
        url: ''
      }
    },
    
    addColumn() {
      this.createForm.columns.push({ name: '', type: 'VARCHAR', nullable: true, primaryKey: false })
    },
    
    removeColumn(idx) {
      if (this.createForm.columns.length > 1) {
        this.createForm.columns.splice(idx, 1)
      }
    },
    
    async executeCreate() {
      try {
        let response
        const options = {
          if_not_exists: this.createForm.ifNotExists,
          comment: this.createForm.comment || undefined
        }
        
        switch (this.selectedType) {
          case 'database':
            response = await axios.post('/api/objects/databases', {
              name: this.createForm.name,
              options: {
                ...options,
                transient: this.createForm.transient
              }
            })
            break
            
          case 'schema':
            response = await axios.post('/api/objects/schemas', {
              database: this.createForm.database,
              name: this.createForm.name,
              options
            })
            break
            
          case 'table':
            const columns = this.createForm.columns
              .filter(c => c.name)
              .map(c => ({
                name: c.name,
                type: c.type,
                nullable: c.nullable,
                primary_key: c.primaryKey
              }))
            
            response = await axios.post('/api/objects/tables', {
              database: this.createForm.database,
              schema_name: this.createForm.schema,
              name: this.createForm.name,
              columns,
              options
            })
            break
            
          case 'view':
            response = await axios.post('/api/objects/views', {
              database: this.createForm.database,
              schema_name: this.createForm.schema,
              name: this.createForm.name,
              definition: this.createForm.definition,
              options: {
                ...options,
                secure: this.createForm.secure
              }
            })
            break
            
          case 'stage':
            response = await axios.post('/api/objects/stages', {
              database: this.createForm.database,
              schema_name: this.createForm.schema,
              name: this.createForm.name,
              options: {
                ...options,
                url: this.createForm.url || undefined
              }
            })
            break
        }
        
        if (response.data.success) {
          this.showMessage(`${this.currentTypeName} created successfully`, 'success')
          this.showCreateModal = false
          this.resetForm()
          await this.fetchDatabases()
          await this.refreshObjects()
        } else {
          this.showMessage(response.data.error || 'Creation failed', 'error')
        }
      } catch (error) {
        this.showMessage(error.response?.data?.error || error.message, 'error')
      }
    },
    
    editObject(obj) {
      // For now, show DDL - could expand to full edit form
      this.viewDDL(obj)
    },
    
    async viewDDL(obj) {
      this.ddlObject = obj
      this.ddlContent = 'Loading...'
      this.showDDLModal = true
      
      try {
        const database = obj.database || obj.name
        const schema = obj.schema || 'PUBLIC'
        const name = obj.name
        
        const response = await axios.get(
          `/api/objects/${this.selectedType}/${database}/${schema}/${name}/ddl`
        )
        
        if (response.data.success) {
          this.ddlContent = response.data.ddl
        } else {
          this.ddlContent = `-- Unable to get DDL\n-- ${response.data.error}`
        }
      } catch (error) {
        this.ddlContent = `-- Error: ${error.message}`
      }
    },
    
    async copyDDL() {
      try {
        await navigator.clipboard.writeText(this.ddlContent)
        this.showMessage('DDL copied to clipboard', 'success')
      } catch (error) {
        console.error('Failed to copy:', error)
      }
    },
    
    deleteObject(obj) {
      this.deleteTarget = obj
      this.showDeleteModal = true
    },
    
    async confirmDelete() {
      if (!this.deleteTarget) return
      
      try {
        const obj = this.deleteTarget
        let url
        
        switch (this.selectedType) {
          case 'database':
            url = `/api/objects/databases/${obj.name}?if_exists=true`
            break
          case 'schema':
            url = `/api/objects/databases/${obj.database}/schemas/${obj.name}?if_exists=true`
            break
          case 'table':
            url = `/api/objects/databases/${obj.database}/schemas/${obj.schema}/tables/${obj.name}?if_exists=true`
            break
          case 'view':
            url = `/api/objects/databases/${obj.database}/schemas/${obj.schema}/views/${obj.name}?if_exists=true`
            break
          case 'stage':
            url = `/api/objects/databases/${obj.database}/schemas/${obj.schema}/stages/${obj.name}?if_exists=true`
            break
        }
        
        const response = await axios.delete(url)
        
        if (response.data.success) {
          this.showMessage(`${this.currentTypeName} deleted successfully`, 'success')
          this.showDeleteModal = false
          this.deleteTarget = null
          await this.fetchDatabases()
          await this.refreshObjects()
        } else {
          this.showMessage(response.data.error || 'Deletion failed', 'error')
        }
      } catch (error) {
        this.showMessage(error.response?.data?.error || error.message, 'error')
      }
    },
    
    getObjectIcon(obj) {
      if (obj.type === 'VIEW') return '👁️'
      return this.objectTypes.find(t => t.id === this.selectedType)?.icon || '📄'
    },
    
    formatDate(dateStr) {
      if (!dateStr) return 'N/A'
      return new Date(dateStr).toLocaleDateString()
    },
    
    showMessage(text, type = 'info') {
      this.message = { text, type }
      setTimeout(() => {
        if (this.message?.text === text) {
          this.message = null
        }
      }, 5000)
    }
  },
  async mounted() {
    await this.fetchDatabases()
  }
}
</script>

<style scoped>
.object-manager {
  padding: 1.5rem;
}

.panel-header {
  margin-bottom: 1.5rem;
}

.section-title {
  font-size: 1.25rem;
  font-weight: 600;
  margin-bottom: 0.25rem;
}

.description {
  color: var(--text-muted);
  font-size: 0.9rem;
}

.object-tabs {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 1.5rem;
  flex-wrap: wrap;
}

.tab-btn {
  padding: 0.75rem 1.25rem;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: var(--card-bg);
  cursor: pointer;
  font-size: 0.9rem;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.tab-btn:hover {
  border-color: var(--primary-color);
}

.tab-btn.active {
  background: var(--primary-color);
  color: white;
  border-color: var(--primary-color);
}

.tab-icon {
  font-size: 1.1rem;
}

.action-bar {
  display: flex;
  gap: 0.75rem;
  margin-bottom: 1.5rem;
}

.btn-primary {
  background: var(--primary-color);
  color: white;
  border: none;
  padding: 0.75rem 1.25rem;
  border-radius: 8px;
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
  padding: 0.75rem 1.25rem;
  border-radius: 8px;
  font-weight: 500;
  cursor: pointer;
}

.btn-danger {
  background: #ef4444;
  color: white;
  border: none;
  padding: 0.75rem 1.25rem;
  border-radius: 8px;
  font-weight: 500;
  cursor: pointer;
}

.objects-list {
  background: var(--card-bg);
  border-radius: 12px;
  padding: 1.5rem;
  border: 1px solid var(--border-color);
  min-height: 300px;
}

.loading, .empty-state {
  text-align: center;
  padding: 3rem;
  color: var(--text-muted);
}

.objects-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 1rem;
}

.object-card {
  background: var(--bg-color);
  border-radius: 10px;
  padding: 1rem;
  border: 1px solid var(--border-color);
  transition: all 0.2s ease;
}

.object-card:hover {
  border-color: var(--primary-color);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.card-header {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  margin-bottom: 0.75rem;
}

.obj-icon {
  font-size: 1.5rem;
}

.obj-name {
  font-weight: 600;
  font-size: 1rem;
  word-break: break-all;
}

.card-body {
  font-size: 0.85rem;
  color: var(--text-muted);
}

.card-body p {
  margin: 0.25rem 0;
}

.card-actions {
  display: flex;
  gap: 0.5rem;
  margin-top: 1rem;
  padding-top: 0.75rem;
  border-top: 1px solid var(--border-color);
}

.action-btn {
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  background: var(--card-bg);
  cursor: pointer;
  font-size: 0.9rem;
}

.action-btn:hover {
  background: var(--bg-color);
}

.action-btn.danger:hover {
  background: #fee2e2;
  border-color: #ef4444;
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
  max-width: 700px;
  width: 90%;
  max-height: 85vh;
  overflow-y: auto;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
}

.modal-content.modal-small {
  max-width: 450px;
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1.25rem 1.5rem;
  border-bottom: 1px solid var(--border-color);
}

.modal-header h3 {
  margin: 0;
  font-size: 1.1rem;
}

.close-btn {
  background: none;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  color: var(--text-muted);
}

.modal-body {
  padding: 1.5rem;
}

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: 0.75rem;
  padding: 1rem 1.5rem;
  border-top: 1px solid var(--border-color);
}

.form-section {
  margin-bottom: 1rem;
}

.form-row {
  display: flex;
  gap: 1rem;
}

.form-group {
  flex: 1;
  margin-bottom: 1rem;
}

.form-group label {
  display: block;
  font-size: 0.9rem;
  font-weight: 500;
  margin-bottom: 0.5rem;
}

.form-group.checkbox label {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  cursor: pointer;
}

.form-group input[type="text"],
.form-group input[type="number"],
.form-group select {
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

.columns-editor {
  margin-top: 1rem;
  padding: 1rem;
  background: var(--bg-color);
  border-radius: 8px;
}

.columns-editor h4 {
  margin: 0 0 0.75rem 0;
}

.column-row {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 0.5rem;
  align-items: center;
  flex-wrap: wrap;
}

.col-name {
  flex: 2;
  min-width: 120px;
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
}

.col-type {
  flex: 1;
  min-width: 120px;
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
}

.col-nullable, .col-pk {
  display: flex;
  align-items: center;
  gap: 0.25rem;
  font-size: 0.85rem;
  cursor: pointer;
}

.remove-col-btn {
  background: #fee2e2;
  border: none;
  color: #ef4444;
  width: 28px;
  height: 28px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 1rem;
}

.add-col-btn {
  margin-top: 0.5rem;
  padding: 0.5rem 1rem;
  background: var(--card-bg);
  border: 1px dashed var(--border-color);
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.85rem;
}

.ddl-content {
  background: #1e1e1e;
  color: #d4d4d4;
  padding: 1rem;
  border-radius: 8px;
  overflow-x: auto;
  font-size: 0.9rem;
  max-height: 400px;
}

.warning {
  color: #d97706;
  font-size: 0.9rem;
}

.message {
  position: fixed;
  bottom: 1.5rem;
  right: 1.5rem;
  padding: 1rem 1.5rem;
  border-radius: 8px;
  display: flex;
  align-items: center;
  gap: 1rem;
  z-index: 1100;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
}

.message.success {
  background: #dcfce7;
  color: #16a34a;
}

.message.error {
  background: #fee2e2;
  color: #dc2626;
}

.message.info {
  background: #e0f2fe;
  color: #0284c7;
}

.close-msg {
  background: none;
  border: none;
  font-size: 1.2rem;
  cursor: pointer;
  opacity: 0.7;
}
</style>
