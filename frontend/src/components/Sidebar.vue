<template>
  <aside :class="['sidebar', { collapsed: collapsed }]">
    <!-- Sidebar Header with Toggle -->
    <div class="sidebar-header">
      <button class="collapse-btn" @click="$emit('toggle-collapse')" :title="collapsed ? 'Expand sidebar' : 'Collapse sidebar'">
        <span v-if="!collapsed">◀</span>
        <span v-else>▶</span>
      </button>
    </div>
    
    <!-- Navigation Items -->
    <nav class="sidebar-nav">
      <div class="nav-section">
        <div v-if="!collapsed" class="nav-section-title">WORKSPACE</div>
        <button 
          v-for="item in workspaceItems" 
          :key="item.id"
          :class="['nav-item', { active: activeView === item.id }]"
          @click="$emit('navigate', item.id)"
          :title="collapsed ? item.label : ''"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span v-if="!collapsed" class="nav-label">{{ item.label }}</span>
          <span v-if="!collapsed && item.badge" class="nav-badge">{{ item.badge }}</span>
        </button>
      </div>

      <div class="nav-section">
        <div v-if="!collapsed" class="nav-section-title">DATA</div>
        <button 
          v-for="item in dataItems" 
          :key="item.id"
          :class="['nav-item', { active: activeView === item.id }]"
          @click="$emit('navigate', item.id)"
          :title="collapsed ? item.label : ''"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span v-if="!collapsed" class="nav-label">{{ item.label }}</span>
        </button>
      </div>

      <div class="nav-section">
        <div v-if="!collapsed" class="nav-section-title">MONITORING</div>
        <button 
          v-for="item in monitoringItems" 
          :key="item.id"
          :class="['nav-item', { active: activeView === item.id }]"
          @click="$emit('navigate', item.id)"
          :title="collapsed ? item.label : ''"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span v-if="!collapsed" class="nav-label">{{ item.label }}</span>
        </button>
      </div>

      <div class="nav-section">
        <div v-if="!collapsed" class="nav-section-title">ADMIN</div>
        <button 
          v-for="item in adminItems" 
          :key="item.id"
          :class="['nav-item', { active: activeView === item.id }]"
          @click="$emit('navigate', item.id)"
          :title="collapsed ? item.label : ''"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span v-if="!collapsed" class="nav-label">{{ item.label }}</span>
        </button>
      </div>
    </nav>

    <!-- Sidebar Footer -->
    <div v-if="!collapsed" class="sidebar-footer">
      <div class="connection-info">
        <div class="info-label">Active Sessions</div>
        <div class="info-value">{{ stats.active_sessions || 0 }}</div>
      </div>
      <div class="connection-info">
        <div class="info-label">Queries</div>
        <div class="info-value">{{ stats.total_queries || 0 }}</div>
      </div>
      <button class="refresh-btn" @click="$emit('refresh')" title="Refresh">
        <span :class="{ 'rotating': isRefreshing }">🔄</span>
        Refresh
      </button>
    </div>
    
    <div v-else class="sidebar-footer-compact">
      <button class="refresh-btn-compact" @click="$emit('refresh')" title="Refresh">
        <span :class="{ 'rotating': isRefreshing }">🔄</span>
      </button>
    </div>
  </aside>
</template>

<script>
export default {
  name: 'Sidebar',
  props: {
    collapsed: Boolean,
    activeView: String,
    stats: Object,
    isRefreshing: Boolean,
    worksheetCount: {
      type: Number,
      default: 1
    }
  },
  computed: {
    workspaceItems() {
      return [
        { id: 'worksheets', label: 'Worksheets', icon: '📝', badge: this.worksheetCount },
        { id: 'overview', label: 'Home', icon: '🏠' },
      ]
    },
    dataItems() {
      return [
        { id: 'databases', label: 'Databases', icon: '🗄️' },
        { id: 'schemas', label: 'Schemas', icon: '📁' },
        { id: 'tables', label: 'Tables', icon: '📊' },
        { id: 'views', label: 'Views', icon: '👁️' },
        { id: 'stages', label: 'Stages', icon: '📦' },
      ]
    },
    monitoringItems() {
      return [
        { id: 'queries', label: 'Query History', icon: '🕒' },
        { id: 'sessions', label: 'Sessions', icon: '🔗' },
        { id: 'logs', label: 'Server Logs', icon: '📋' },
      ]
    },
    adminItems() {
      return [
        { id: 'warehouses', label: 'Warehouses', icon: '⚡' },
        { id: 'settings', label: 'Settings', icon: '⚙️' },
      ]
    }
  }
}
</script>

<style scoped>
.sidebar {
  width: 240px;
  background: var(--card-bg);
  border-right: 1px solid var(--border-color);
  display: flex;
  flex-direction: column;
  overflow-y: auto;
  transition: width 0.3s ease;
}

.sidebar.collapsed {
  width: 64px;
}

.sidebar-header {
  padding: 0.5rem;
  border-bottom: 1px solid var(--border-color);
  display: flex;
  justify-content: flex-end;
}

.collapse-btn {
  padding: 0.5rem;
  background: none;
  border: 1px solid var(--border-color);
  border-radius: 6px;
  cursor: pointer;
  color: var(--text-muted);
  transition: all 0.2s ease;
  font-size: 0.9rem;
}

.collapse-btn:hover {
  background: var(--bg-color);
  color: var(--text-color);
}

.sidebar-nav {
  flex: 1;
  padding: 0.5rem;
}

.nav-section {
  margin-bottom: 1rem;
}

.nav-section-title {
  font-size: 0.7rem;
  font-weight: 600;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  padding: 0.5rem 1rem;
  margin-bottom: 0.25rem;
}

.nav-item {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.625rem 1rem;
  background: none;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  color: var(--text-muted);
  font-size: 0.9rem;
  transition: all 0.2s ease;
  margin-bottom: 0.125rem;
  text-align: left;
}

.nav-item:hover {
  background: var(--bg-color);
  color: var(--text-color);
}

.nav-item.active {
  background: linear-gradient(135deg, rgba(41, 181, 232, 0.15), rgba(26, 163, 217, 0.15));
  color: var(--primary-color);
  font-weight: 500;
}

.nav-icon {
  font-size: 1.1rem;
  width: 24px;
  text-align: center;
}

.nav-label {
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.nav-badge {
  background: var(--primary-color);
  color: white;
  font-size: 0.7rem;
  padding: 0.125rem 0.375rem;
  border-radius: 10px;
  font-weight: 600;
}

.sidebar.collapsed .nav-item {
  justify-content: center;
  padding: 0.75rem 0.5rem;
}

.sidebar.collapsed .nav-icon {
  margin: 0;
}

.sidebar-footer-compact {
  padding: 0.5rem;
  border-top: 1px solid var(--border-color);
  background: var(--bg-color);
  display: flex;
  justify-content: center;
}

.refresh-btn-compact {
  padding: 0.5rem;
  background: var(--primary-color);
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 1rem;
  transition: all 0.2s ease;
  width: 40px;
  height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.refresh-btn-compact:hover {
  opacity: 0.9;
  transform: translateY(-1px);
}

.sidebar-footer {
  padding: 1rem;
  border-top: 1px solid var(--border-color);
  background: var(--bg-color);
}

.connection-info {
  display: flex;
  justify-content: space-between;
  margin-bottom: 0.5rem;
  font-size: 0.85rem;
}

.info-label {
  color: var(--text-muted);
}

.info-value {
  font-weight: 600;
  color: var(--primary-color);
}

.refresh-btn {
  width: 100%;
  padding: 0.5rem;
  background: var(--primary-color);
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.9rem;
  margin-top: 0.5rem;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.5rem;
}

.refresh-btn:hover {
  opacity: 0.9;
  transform: translateY(-1px);
}

.rotating {
  display: inline-block;
  animation: rotate 1s linear infinite;
}

@keyframes rotate {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
</style>
