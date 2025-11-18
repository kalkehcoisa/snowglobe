# Snowglobe Cleanup Guide

This guide explains the new cleanup scripts and how to use them to maintain a clean build environment.

## Why Cleanup is Important

When building Docker images and frontends, many artifacts are cached:
- Docker build layers and images
- Node.js dependencies (`node_modules`)
- Python bytecode (`__pycache__`, `.pyc` files)
- Build artifacts and temporary files
- System cache files

**These cached files can cause issues when:**
- Frontend changes don't appear after rebuild
- Docker images contain old code
- Disk space runs out
- Build processes fail mysteriously

## Available Cleanup Commands

### Quick Reference

```bash
# Complete cleanup (recommended for fresh start)
make clean-all

# Just Docker cleanup
make clean-docker

# Just frontend cleanup
make clean-frontend

# Just Python artifacts
make clean-python

# Just cache files
make clean-cache

# Clean and rebuild everything
make rebuild
```

## Detailed Command Descriptions

### 1. `make clean-all` - Complete Cleanup

**Use when:** You want a completely fresh start or frontend changes aren't showing up.

**What it does:**
- Stops and removes all Docker containers
- Removes all Docker images related to Snowglobe
- Cleans Docker build cache
- Removes `node_modules` and frontend build artifacts
- Removes Python `__pycache__` and `.pyc` files
- Removes all cache and temporary files
- **Optionally** removes data directory (asks for confirmation)
- **Optionally** removes SSL certificates (asks for confirmation)

**Usage:**
```bash
make clean-all
# or directly:
./clean-all.sh
```

**⚠️ Warning:** This will stop your running Snowglobe instance!

---

### 2. `make clean-docker` - Docker-Only Cleanup

**Use when:** You need to rebuild Docker images without touching other files.

**What it does:**
- Stops Docker containers
- Removes Snowglobe containers
- Removes Snowglobe images
- Cleans Docker build cache
- Prunes dangling images and volumes

**Usage:**
```bash
make clean-docker
# or directly:
./clean-docker.sh
```

**When to use:**
- Backend Python code changes not appearing
- Docker build is using old layers
- Need to free Docker disk space

---

### 3. `make clean-frontend` - Frontend-Only Cleanup

**Use when:** Frontend changes aren't appearing or `npm install` is broken.

**What it does:**
- Removes `node_modules` directory
- Removes `dist` build directory
- Removes `.vite` cache
- Removes lock files (`package-lock.json`, `yarn.lock`)
- Removes frontend cache directories

**Usage:**
```bash
make clean-frontend
# or directly:
./clean-frontend.sh
```

**When to use:**
- Frontend changes not appearing after rebuild
- Node dependencies are corrupted
- Need to reinstall all npm packages

---

### 4. `make clean-python` - Python Artifacts Cleanup

**Use when:** Python import issues or pytest fails oddly.

**What it does:**
- Removes `__pycache__` directories
- Removes `.pyc` and `.pyo` files
- Removes `.egg-info` directories
- Removes pytest cache
- Removes coverage reports
- Removes build/dist directories

**Usage:**
```bash
make clean-python
```

**When to use:**
- Python import errors
- Old test results cached
- Module changes not being picked up

---

### 5. `make clean-cache` - System Cache Cleanup

**Use when:** Cleaning up system-specific cache files.

**What it does:**
- Removes `.DS_Store` files (macOS)
- Removes `._*` files (macOS resource forks)
- Removes `Thumbs.db` files (Windows)
- Removes vim swap files
- Removes backup files (`*~`)

**Usage:**
```bash
make clean-cache
```

---

### 6. `make rebuild` - Clean and Rebuild Everything

**Use when:** Starting fresh with a complete rebuild.

**What it does:**
1. Runs `make clean-all`
2. Installs frontend dependencies
3. Builds frontend
4. Builds Docker image (no cache)

**Usage:**
```bash
make rebuild
```

**This is equivalent to:**
```bash
make clean-all
cd frontend && npm install && npm run build
make build
```

---

## Common Scenarios

### Scenario 1: Frontend Changes Not Showing

**Problem:** You modified frontend code but changes don't appear after rebuild.

**Solution:**
```bash
# Option 1: Clean frontend and rebuild
make clean-frontend
cd frontend && npm install && npm run build
make build
make restart

# Option 2: Complete rebuild (safest)
make rebuild
make start
```

---

### Scenario 2: Docker Image Contains Old Code

**Problem:** Backend Python changes aren't appearing in Docker container.

**Solution:**
```bash
# Clean Docker and rebuild
make clean-docker
make build
make start
```

---

### Scenario 3: Running Out of Disk Space

**Problem:** Docker is using too much disk space.

**Solution:**
```bash
# Clean Docker artifacts
make clean-docker

# For even more space, clean everything
make clean-all
```

---

### Scenario 4: Fresh Development Setup

**Problem:** Setting up for the first time or after pulling updates.

**Solution:**
```bash
# Complete rebuild
make rebuild
make start
```

---

### Scenario 5: Tests Failing Mysteriously

**Problem:** Tests pass sometimes, fail other times, or import errors.

**Solution:**
```bash
# Clean Python artifacts
make clean-python

# Or full cleanup
make clean-all
```

---

## Best Practices

### Before Making Changes

When you're about to make significant changes:
```bash
# Create a backup (optional)
make backup

# Clean everything for fresh start
make clean-all
```

### After Pulling Updates

When you pull changes from git:
```bash
# Rebuild to ensure all changes are applied
make rebuild
make start
```

### Regular Maintenance

Run cleanup periodically:
```bash
# Weekly: Clean Docker cache
make clean-docker

# Monthly: Complete cleanup
make clean-all
```

### Before Reporting Issues

Before reporting bugs, try a clean rebuild:
```bash
make rebuild
make start
```

This eliminates cache-related issues.

---

## Understanding What Gets Removed

### Safe to Remove (Always)

These can be regenerated and are safe to remove:
- `__pycache__/` - Python bytecode cache
- `node_modules/` - Node.js dependencies
- `dist/` - Build output
- `.pyc`, `.pyo` - Python bytecode files
- Docker images and containers
- `.DS_Store`, `Thumbs.db` - System files

### Requires Confirmation

These contain user data and scripts ask for confirmation:
- `data/` - Your database files ⚠️
- `certs/` - SSL certificates
- `backups/` - Database backups

### Never Removed

These are never touched by cleanup scripts:
- Source code (`.py`, `.js`, `.jsx` files)
- Configuration files
- Documentation
- Tests

---

## Troubleshooting Cleanup

### "Permission Denied" Errors

If you get permission errors:
```bash
# On Linux/macOS, use sudo
sudo make clean-docker

# Or fix permissions
sudo chown -R $USER:$USER .
```

### Docker Won't Stop

If Docker containers won't stop:
```bash
# Force stop all containers
docker stop $(docker ps -a -q)

# Force remove all containers
docker rm -f $(docker ps -a -q)
```

### Can't Delete Files

If files can't be deleted:
```bash
# On Linux/macOS
chmod -R 755 .
rm -rf node_modules __pycache__ dist
```

---

## Script Locations

All cleanup scripts are in the project root:
- `clean-all.sh` - Complete cleanup
- `clean-docker.sh` - Docker cleanup
- `clean-frontend.sh` - Frontend cleanup

These scripts are also accessible via Makefile targets.

---

## Environment Variables

Cleanup scripts respect these environment variables:

```bash
# Force cleanup without confirmation
export SNOWGLOBE_FORCE_CLEAN=1
make clean-all

# Keep data directory
export SNOWGLOBE_KEEP_DATA=1
make clean-all
```

---

## Summary

| Command | Use Case | Safety Level |
|---------|----------|--------------|
| `make clean-all` | Fresh start | ⚠️ Asks for confirmation |
| `make clean-docker` | Docker issues | ✅ Safe |
| `make clean-frontend` | Frontend issues | ✅ Safe |
| `make clean-python` | Python issues | ✅ Safe |
| `make clean-cache` | Cache files | ✅ Safe |
| `make rebuild` | Complete rebuild | ⚠️ Asks for confirmation |

**Golden Rule:** When in doubt, run `make rebuild` to ensure a clean build! 🚀
