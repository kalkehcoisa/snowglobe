# Changelog

All notable changes to Snowglobe will be documented in this file.

## [0.2.0] - 2024-01-XX - Enhanced Edition

### 🎉 Major Features

#### SSL/TLS/HTTPS Support
- ✅ Full HTTPS encryption support
- ✅ Auto-generation of self-signed SSL certificates
- ✅ Support for custom SSL certificates
- ✅ Dual protocol support (HTTP + HTTPS)
- ✅ TLS 1.2+ implementation
- ✅ Snowflake-standard secure connections

#### New User Interface
- ✅ Side menu navigation
- ✅ Modern, responsive design
- ✅ Six main views: Worksheet, Overview, Query History, Sessions, Databases, Settings
- ✅ Dark theme SQL editor
- ✅ Real-time updates
- ✅ Improved mobile responsiveness

#### SQL Worksheet
- ✅ Snowflake-like query interface
- ✅ Syntax-highlighted code editor
- ✅ Sample queries for quick start
- ✅ Query result pagination
- ✅ CSV export functionality
- ✅ Keyboard shortcuts (Ctrl/Cmd+Enter)
- ✅ Query formatting
- ✅ Multi-line query support

#### Settings Panel
- ✅ Server information display
- ✅ Connection details
- ✅ Performance metrics
- ✅ Security status
- ✅ Environment configuration guide
- ✅ Python connection examples

### 🔧 Improvements

#### Backend
- ✅ Enhanced server with dual-protocol support
- ✅ Better error handling
- ✅ Improved logging
- ✅ Query execution endpoint for frontend
- ✅ Health check enhancements

#### Docker
- ✅ Auto-generation of SSL certificates in Dockerfile
- ✅ Improved environment variable support
- ✅ Better health checks
- ✅ Volume management for certificates
- ✅ Non-root user execution

#### Documentation
- ✅ Comprehensive README with HTTPS setup guide
- ✅ Python connection examples
- ✅ Docker configuration guide
- ✅ SSL certificate management guide
- ✅ Environment variables documentation

#### Scripts
- ✅ `quickstart.sh` - One-command setup and launch
- ✅ `generate-certs.sh` - Interactive SSL certificate generation
- ✅ Makefile with common commands
- ✅ Example Python scripts

### 📦 New Dependencies
- None (all dependencies remain the same)

### 🐛 Bug Fixes
- Fixed query history overflow
- Improved session management
- Better error messages
- Fixed CSS rendering issues

---

## [0.1.0] - 2024-01-XX - Initial Release

### Features
- ✅ Snowflake Python Connector compatibility
- ✅ DuckDB backend
- ✅ Basic SQL support (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP)
- ✅ Database and schema management
- ✅ Session management
- ✅ Query execution and history
- ✅ Web dashboard
- ✅ Docker support
- ✅ FastAPI server
- ✅ Vue.js frontend

### Components
- FastAPI-based HTTP server
- DuckDB for data storage
- SQL query translator
- Metadata management system
- Basic web interface
- Docker containerization

---

## Future Roadmap

### v0.3.0 (Planned)
- [ ] User authentication system
- [ ] Role-based access control (RBAC)
- [ ] Advanced SQL features (CTEs, window functions)
- [ ] Stored procedures support
- [ ] UDF (User-Defined Functions) support
- [ ] Stage and file upload support
- [ ] Data loading from S3
- [ ] Query optimization
- [ ] Multi-warehouse simulation

### v0.4.0 (Planned)
- [ ] Clustering and partitioning
- [ ] Time travel and zero-copy cloning
- [ ] Materialized views
- [ ] Streams and tasks
- [ ] External tables
- [ ] Data sharing simulation
- [ ] Advanced monitoring and metrics
- [ ] Cost estimation

### Community Requests
- [ ] PostgreSQL backend option
- [ ] SQLite backend option
- [ ] Integration with dbt
- [ ] Integration with Airflow
- [ ] REST API for management
- [ ] GraphQL support
- [ ] WebSocket for real-time updates
- [ ] Query profiling and explain plans

---

## Contributing

We welcome contributions! Please see CONTRIBUTING.md for guidelines.

## Version History

- **0.2.0** - Enhanced edition with HTTPS, new UI, and SQL Worksheet
- **0.1.0** - Initial release with basic Snowflake emulation

---

## Support

For bug reports and feature requests, please open an issue on GitHub.
