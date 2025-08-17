.PHONY: install test typecheck lint format run clean help

# Default target
help:
	@echo "Crypto Arbitrage Bot - Available Commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make test      - Run test suite"
	@echo "  make typecheck - Run mypy type checking"
	@echo "  make lint      - Run ruff linting"
	@echo "  make format    - Format code with black"
	@echo "  make run       - Run the arbitrage bot"
	@echo "  make setup     - Run setup and test script"
	@echo "  make clean     - Clean up cache files"
	@echo "  make help      - Show this help"

# Install dependencies
install:
	pip install -r requirements.txt

# Run tests
test:
	python -m pytest tests/ -v

# Type checking
typecheck:
	python -m mypy src/

# Linting
lint:
	python -m ruff check src/

# Format code
format:
	python -m black src/ tests/
	python -m ruff check --fix src/

# Run the bot
run:
	python src/app.py

# Setup and test everything
setup:
	python setup_and_test.py

# Clean cache files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -f arbitrage.db arbitrage_bot.log
