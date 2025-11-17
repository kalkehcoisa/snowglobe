#!/bin/bash

# Snowglobe Test Runner
# This script provides easy access to common test scenarios

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored message
print_msg() {
    color=$1
    shift
    echo -e "${color}$@${NC}"
}

# Print usage
usage() {
    cat << EOF
Snowglobe Test Runner

Usage: $0 [OPTION]

Options:
    all             Run all tests
    unit            Run unit tests only
    integration     Run integration tests only
    api             Run API endpoint tests
    ssl             Run SSL/TLS tests
    edge            Run edge case tests
    performance     Run performance tests (slow)
    coverage        Run tests with coverage report
    fast            Run fast tests (exclude slow tests)
    verbose         Run tests with verbose output
    debug           Run tests with debug output
    watch           Watch mode - re-run tests on file changes
    docker          Run tests in Docker container
    help            Show this help message

Examples:
    $0 all                  # Run all tests
    $0 unit                 # Run only unit tests
    $0 coverage             # Run with coverage
    $0 api verbose          # Run API tests with verbose output

EOF
    exit 0
}

# No arguments
if [ $# -eq 0 ]; then
    usage
fi

# Parse arguments
PYTEST_ARGS=""
RUN_DOCKER=false

while [ $# -gt 0 ]; do
    case $1 in
        all)
            print_msg "$BLUE" "Running all tests..."
            PYTEST_ARGS="$PYTEST_ARGS tests/"
            ;;
        unit)
            print_msg "$BLUE" "Running unit tests..."
            PYTEST_ARGS="$PYTEST_ARGS -m unit"
            ;;
        integration)
            print_msg "$BLUE" "Running integration tests..."
            PYTEST_ARGS="$PYTEST_ARGS -m integration"
            ;;
        api)
            print_msg "$BLUE" "Running API tests..."
            PYTEST_ARGS="$PYTEST_ARGS -m api"
            ;;
        ssl)
            print_msg "$BLUE" "Running SSL/TLS tests..."
            PYTEST_ARGS="$PYTEST_ARGS -m ssl"
            ;;
        edge)
            print_msg "$BLUE" "Running edge case tests..."
            PYTEST_ARGS="$PYTEST_ARGS -m edge_case"
            ;;
        performance)
            print_msg "$YELLOW" "Running performance tests (this may take a while)..."
            PYTEST_ARGS="$PYTEST_ARGS -m performance"
            ;;
        coverage)
            print_msg "$BLUE" "Running tests with coverage..."
            PYTEST_ARGS="$PYTEST_ARGS --cov=snowglobe_server --cov-report=term-missing --cov-report=html"
            ;;
        fast)
            print_msg "$BLUE" "Running fast tests only..."
            PYTEST_ARGS="$PYTEST_ARGS -m 'not slow and not performance'"
            ;;
        verbose)
            PYTEST_ARGS="$PYTEST_ARGS -v"
            ;;
        debug)
            PYTEST_ARGS="$PYTEST_ARGS -vv -s"
            ;;
        watch)
            print_msg "$BLUE" "Starting watch mode..."
            if ! command -v ptw &> /dev/null; then
                print_msg "$YELLOW" "Installing pytest-watch..."
                pip install pytest-watch
            fi
            ptw $PYTEST_ARGS
            exit 0
            ;;
        docker)
            RUN_DOCKER=true
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            print_msg "$RED" "Unknown option: $1"
            usage
            ;;
    esac
    shift
done

# Run tests
if [ "$RUN_DOCKER" = true ]; then
    print_msg "$BLUE" "Running tests in Docker..."
    docker-compose run --rm test pytest $PYTEST_ARGS
else
    # Check if pytest is installed
    if ! command -v pytest &> /dev/null; then
        print_msg "$RED" "pytest is not installed!"
        print_msg "$YELLOW" "Installing test dependencies..."
        pip install pytest pytest-cov pytest-asyncio httpx
    fi
    
    # Run pytest
    print_msg "$GREEN" "Executing: pytest $PYTEST_ARGS"
    pytest $PYTEST_ARGS
    
    # Print coverage info if HTML report was generated
    if [[ "$PYTEST_ARGS" == *"--cov-report=html"* ]]; then
        print_msg "$GREEN" "Coverage report generated at: htmlcov/index.html"
    fi
fi

print_msg "$GREEN" "✓ Tests completed!"
