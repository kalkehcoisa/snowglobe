#!/bin/bash
###############################################################################
# Pre-Build Script
# Runs before building to ensure clean environment
###############################################################################

set -e

echo "🔍 Running pre-build checks..."
echo ""

# Check if running in CI environment
if [ "$CI" = "true" ]; then
    echo "✓ Running in CI environment - skipping interactive checks"
    SKIP_INTERACTIVE=1
else
    SKIP_INTERACTIVE=0
fi

# Function to check and report
check_and_report() {
    local name=$1
    local path=$2
    
    if [ -e "$path" ]; then
        echo "  ⚠️  Found: $name"
        return 1
    else
        echo "  ✓ Clean: $name"
        return 0
    fi
}

# Check for artifacts that might cause issues
echo "Checking for old build artifacts..."
has_issues=0

check_and_report "Python cache" "__pycache__" || has_issues=1
check_and_report "Frontend node_modules" "frontend/node_modules" || has_issues=1
check_and_report "Frontend dist" "frontend/dist" || has_issues=1
check_and_report "Python eggs" "*.egg-info" || has_issues=1

echo ""

# If artifacts found, suggest cleanup
if [ $has_issues -eq 1 ]; then
    echo "⚠️  Old build artifacts detected!"
    echo ""
    echo "These artifacts might cause:"
    echo "  - Outdated code in the build"
    echo "  - Frontend changes not appearing"
    echo "  - Build failures"
    echo ""
    
    if [ $SKIP_INTERACTIVE -eq 0 ]; then
        read -p "Would you like to clean them now? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo ""
            echo "🧹 Running cleanup..."
            ./clean-all.sh
            echo ""
            echo "✅ Cleanup complete - ready to build"
        else
            echo ""
            echo "⚠️  Continuing with existing artifacts..."
            echo "   If you experience issues, run: make clean-all"
        fi
    else
        echo "ℹ️  Run 'make clean-all' before building if you experience issues"
    fi
else
    echo "✅ Build environment is clean!"
fi

echo ""
echo "🔍 Checking required files..."

# Check for required files
required_files=(
    "snowglobe_server/server.py"
    "snowglobe_server/query_executor.py"
    "snowglobe_server/metadata.py"
    "snowglobe_server/decorators.py"
    "snowglobe_server/template_loader.py"
    "snowglobe_server/templates/dashboard.html"
    "requirements-server.txt"
    "Dockerfile"
)

all_present=1
for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    else
        echo "  ❌ Missing: $file"
        all_present=0
    fi
done

echo ""

if [ $all_present -eq 0 ]; then
    echo "❌ Some required files are missing!"
    echo "   Please ensure all source files are present"
    exit 1
fi

# Check frontend if it exists
if [ -d "frontend" ]; then
    echo "🔍 Checking frontend..."
    
    if [ ! -f "frontend/package.json" ]; then
        echo "  ❌ frontend/package.json missing!"
        exit 1
    fi
    
    if [ ! -f "frontend/vite.config.js" ]; then
        echo "  ⚠️  frontend/vite.config.js missing (might be optional)"
    fi
    
    echo "  ✓ Frontend structure looks good"
else
    echo "  ℹ️  No frontend directory found (optional)"
fi

echo ""
echo "✅ Pre-build checks complete!"
echo ""
echo "📦 Ready to build Snowglobe"
echo ""
