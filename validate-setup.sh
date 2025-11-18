#!/bin/bash
###############################################################################
# Setup Validation Script
# Validates that all new files and improvements are in place
###############################################################################

set -e

echo "🔍 Validating Snowglobe Setup..."
echo "================================="
echo ""

failed_checks=0
passed_checks=0

# Function to check file exists
check_file() {
    local file=$1
    local description=$2
    
    if [ -f "$file" ]; then
        echo "✅ $description"
        ((passed_checks++))
        return 0
    else
        echo "❌ MISSING: $description - $file"
        ((failed_checks++))
        return 1
    fi
}

# Function to check directory exists
check_dir() {
    local dir=$1
    local description=$2
    
    if [ -d "$dir" ]; then
        echo "✅ $description"
        ((passed_checks++))
        return 0
    else
        echo "❌ MISSING: $description - $dir"
        ((failed_checks++))
        return 1
    fi
}

# Function to check file contains text
check_content() {
    local file=$1
    local text=$2
    local description=$3
    
    if grep -q "$text" "$file" 2>/dev/null; then
        echo "✅ $description"
        ((passed_checks++))
        return 0
    else
        echo "❌ MISSING: $description in $file"
        ((failed_checks++))
        return 1
    fi
}

echo "📁 Checking Core Files..."
echo "-------------------------"
check_file "snowglobe_server/server.py" "Main server file"
check_file "snowglobe_server/query_executor.py" "Query executor"
check_file "snowglobe_server/metadata.py" "Metadata manager"
check_file "snowglobe_server/sql_translator.py" "SQL translator"
check_file "requirements-server.txt" "Server requirements"
check_file "Dockerfile" "Dockerfile"
check_file "docker-compose.yml" "Docker Compose config"
echo ""

echo "🆕 Checking New Improvement Files..."
echo "-------------------------------------"
check_file "snowglobe_server/decorators.py" "Decorators & helpers"
check_file "snowglobe_server/template_loader.py" "Template loader"
check_dir "snowglobe_server/templates" "Templates directory"
check_file "snowglobe_server/templates/dashboard.html" "Dashboard template"
echo ""

echo "🧹 Checking Cleanup Scripts..."
echo "-------------------------------"
check_file "clean-all.sh" "Complete cleanup script"
check_file "clean-docker.sh" "Docker cleanup script"
check_file "clean-frontend.sh" "Frontend cleanup script"
check_file "pre-build.sh" "Pre-build validation script"

# Check if scripts are executable
if [ -x "clean-all.sh" ]; then
    echo "✅ clean-all.sh is executable"
    ((passed_checks++))
else
    echo "⚠️  clean-all.sh is not executable (run: chmod +x clean-all.sh)"
fi

if [ -x "clean-docker.sh" ]; then
    echo "✅ clean-docker.sh is executable"
    ((passed_checks++))
else
    echo "⚠️  clean-docker.sh is not executable (run: chmod +x clean-docker.sh)"
fi

if [ -x "clean-frontend.sh" ]; then
    echo "✅ clean-frontend.sh is executable"
    ((passed_checks++))
else
    echo "⚠️  clean-frontend.sh is not executable (run: chmod +x clean-frontend.sh)"
fi

if [ -x "pre-build.sh" ]; then
    echo "✅ pre-build.sh is executable"
    ((passed_checks++))
else
    echo "⚠️  pre-build.sh is not executable (run: chmod +x pre-build.sh)"
fi
echo ""

echo "📚 Checking Documentation..."
echo "----------------------------"
check_file "CLEANUP_GUIDE.md" "Cleanup guide"
check_file "DEVELOPER_GUIDE.md" "Developer guide"
check_file "CHANGES.md" "Changes documentation"
check_file "QUICK_REFERENCE.md" "Quick reference"
check_file ".dockerignore" "Docker ignore file"
echo ""

echo "🔧 Checking Makefile Updates..."
echo "--------------------------------"
check_content "Makefile" "clean-all" "clean-all target in Makefile"
check_content "Makefile" "clean-docker" "clean-docker target in Makefile"
check_content "Makefile" "clean-frontend" "clean-frontend target in Makefile"
check_content "Makefile" "clean-python" "clean-python target in Makefile"
check_content "Makefile" "rebuild" "rebuild target in Makefile"
check_content "Makefile" "build-fast" "build-fast target in Makefile"
echo ""

echo "🐍 Checking Server Code Updates..."
echo "-----------------------------------"
check_content "snowglobe_server/server.py" "from .decorators import" "Decorators imported in server.py"
check_content "snowglobe_server/server.py" "from .template_loader import" "Template loader imported"
check_content "snowglobe_server/server.py" "SessionManager" "SessionManager usage"
check_content "snowglobe_server/server.py" "QueryHistoryManager" "QueryHistoryManager usage"
check_content "snowglobe_server/server.py" "load_template" "Template loading"
check_content "snowglobe_server/server.py" "@handle_exceptions" "Exception decorator usage"
echo ""

echo "🎨 Checking Decorators Module..."
echo "---------------------------------"
check_content "snowglobe_server/decorators.py" "def handle_exceptions" "handle_exceptions decorator"
check_content "snowglobe_server/decorators.py" "def log_execution_time" "log_execution_time decorator"
check_content "snowglobe_server/decorators.py" "def requires_session" "requires_session decorator"
check_content "snowglobe_server/decorators.py" "class SessionManager" "SessionManager class"
check_content "snowglobe_server/decorators.py" "class QueryHistoryManager" "QueryHistoryManager class"
check_content "snowglobe_server/decorators.py" "def create_success_response" "Success response helper"
check_content "snowglobe_server/decorators.py" "def create_error_response" "Error response helper"
check_content "snowglobe_server/decorators.py" "def get_statement_type_id" "Statement type helper"
echo ""

echo "📄 Checking Template System..."
echo "-------------------------------"
check_content "snowglobe_server/template_loader.py" "class TemplateLoader" "TemplateLoader class"
check_content "snowglobe_server/template_loader.py" "def load_template" "load_template function"
check_content "snowglobe_server/templates/dashboard.html" "<!DOCTYPE html>" "Dashboard HTML structure"
check_content "snowglobe_server/templates/dashboard.html" "Snowglobe Dashboard" "Dashboard title"
echo ""

echo "🔍 Checking .dockerignore..."
echo "----------------------------"
check_content ".dockerignore" "__pycache__" "Python cache ignored"
check_content ".dockerignore" "node_modules" "Node modules ignored"
check_content ".dockerignore" "*.md" "Markdown files ignored"
check_content ".dockerignore" "tests/" "Tests ignored"
echo ""

echo "================================="
echo "📊 Validation Summary"
echo "================================="
echo ""
echo "✅ Passed checks: $passed_checks"
echo "❌ Failed checks: $failed_checks"
echo ""

if [ $failed_checks -eq 0 ]; then
    echo "🎉 All validations passed!"
    echo ""
    echo "✅ Your Snowglobe setup is complete and ready to use!"
    echo ""
    echo "Next steps:"
    echo "  1. Run 'make rebuild' to build everything"
    echo "  2. Run 'make start' to start Snowglobe"
    echo "  3. Visit https://localhost:8443/dashboard"
    echo ""
    echo "📚 Documentation:"
    echo "  - CLEANUP_GUIDE.md    - How to clean build artifacts"
    echo "  - DEVELOPER_GUIDE.md  - Developer documentation"
    echo "  - QUICK_REFERENCE.md  - Quick command reference"
    echo "  - CHANGES.md          - What changed and why"
    echo ""
    exit 0
else
    echo "⚠️  Some validations failed!"
    echo ""
    echo "Please ensure all files are present and properly configured."
    echo "Missing files might have been excluded during extraction."
    echo ""
    echo "To fix:"
    echo "  1. Check that all .sh scripts are present"
    echo "  2. Run: chmod +x *.sh"
    echo "  3. Ensure all documentation files are present"
    echo "  4. Verify snowglobe_server directory structure"
    echo ""
    exit 1
fi
