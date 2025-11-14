.PHONY: help install install-dev install-server test lint format clean docker-build docker-up docker-down docker-logs

help:
	@echo "Snowglobe - Local Snowflake Emulator"
	@echo ""
	@echo "Available commands:"
	@echo "  install         Install client package"
	@echo "  install-dev     Install development dependencies"
	@echo "  install-server  Install server dependencies"
	@echo "  test            Run all tests"
	@echo "  test-unit       Run unit tests only"
	@echo "  test-integration Run integration tests"
	@echo "  lint            Run code linting"
	@echo "  format          Format code"
	@echo "  clean           Clean build artifacts"
	@echo "  docker-build    Build Docker image"
	@echo "  docker-up       Start Docker container"
	@echo "  docker-down     Stop Docker container"
	@echo "  docker-logs     View container logs"
	@echo "  server          Run server locally"

install:
	pip install -e .

install-dev:
	pip install -e .[dev,test]

install-server:
	pip install -e .[server]

test:
	pytest tests/ -v --cov=snowglobe_client --cov=snowglobe_server --cov-report=term-missing

test-unit:
	pytest tests/ -v -m "not integration"

test-integration:
	pytest tests/ -v -m integration

lint:
	flake8 snowglobe_server/ snowglobe_client/ tests/ --max-line-length=100
	mypy snowglobe_server/ snowglobe_client/ --ignore-missing-imports

format:
	black snowglobe_server/ snowglobe_client/ tests/ examples/ --line-length=100
	isort snowglobe_server/ snowglobe_client/ tests/ examples/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.duckdb" -delete
	find . -type f -name "*.duckdb.wal" -delete

docker-build:
	docker build -t snowglobe:latest .

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-clean:
	docker-compose down -v
	docker rmi snowglobe:latest || true

server:
	SNOWGLOBE_PORT=8084 SNOWGLOBE_DATA_DIR=./data SNOWGLOBE_LOG_LEVEL=DEBUG python -m snowglobe_server.server

# Development helpers
dev-setup: install-dev install-server
	@echo "Development environment setup complete!"

check: lint test
	@echo "All checks passed!"

release-check: clean format lint test
	@echo "Ready for release!"
