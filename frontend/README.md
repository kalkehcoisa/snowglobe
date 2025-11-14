# Snowglobe Dashboard

A Vue.js-based debug dashboard for monitoring Snowglobe server sessions, queries, and database structure.

## Features

- **Real-time Overview**: Monitor server health, uptime, and key metrics
- **Query History**: View all executed queries with status, duration, and error details
- **Session Management**: Track active sessions and their context
- **Database Explorer**: Browse databases, schemas, and tables
- **Auto-refresh**: Automatic updates every 5 seconds (can be toggled)

## Quick Start

### Prerequisites

- Node.js 16+ and npm
- Snowglobe server running on port 8084

### Installation

```bash
cd frontend
npm install
```

### Development

```bash
npm run dev
```

The dashboard will be available at http://localhost:3000

### Production Build

```bash
npm run build
```

The built files will be in the `dist` directory and can be served by any static file server.

## Architecture

The frontend communicates with the Snowglobe server through these API endpoints:

- `GET /health` - Server health check
- `GET /api/stats` - Server statistics
- `GET /api/sessions` - List active sessions
- `GET /api/queries` - Query history
- `GET /api/databases` - List databases
- `GET /api/databases/{db}/schemas` - List schemas
- `GET /api/databases/{db}/schemas/{schema}/tables` - List tables
- `DELETE /api/queries/history` - Clear query history

## Components

### OverviewPanel
Displays server statistics including uptime, active sessions, query counts, and success rates.

### QueryHistoryPanel
Shows all executed queries with:
- Success/failure status
- Execution time
- Affected rows
- SQL text (with full view in modal)
- Error messages for failed queries
- Search and filter capabilities

### SessionsPanel
Lists all active sessions with:
- User information
- Database/schema/warehouse/role context
- Session IDs and tokens
- Creation timestamps

### DatabaseExplorer
Hierarchical view of:
- Databases
- Schemas within databases
- Tables within schemas
- Table metadata (columns, types, nullable)

## Customization

### Changing the Server URL

Edit `vite.config.js` to change the proxy target:

```javascript
proxy: {
  '/api': {
    target: 'http://localhost:8084',  // Change this
    changeOrigin: true
  }
}
```

### Adjusting Auto-refresh Interval

In `App.vue`, modify the interval (default is 5000ms):

```javascript
startAutoRefresh() {
  this.refreshInterval = setInterval(() => {
    if (this.autoRefresh) {
      this.fetchAll()
    }
  }, 5000)  // Change this value
}
```

### Theming

Colors are defined as CSS variables in `index.html`:

```css
:root {
  --primary-color: #29b5e8;
  --secondary-color: #1aa3d9;
  --success-color: #10b981;
  --error-color: #ef4444;
  --warning-color: #f59e0b;
  --bg-color: #f8fafc;
  --card-bg: #ffffff;
  --text-color: #1e293b;
  --text-muted: #64748b;
  --border-color: #e2e8f0;
}
```

## Screenshots

The dashboard provides a Snowflake-like interface with:
- Clean, modern design with Snowflake's signature blue color scheme
- Responsive layout that works on different screen sizes
- Interactive elements with hover effects
- Modal dialogs for detailed information

## License

This is part of the Snowglobe project and shares the same MIT license.
