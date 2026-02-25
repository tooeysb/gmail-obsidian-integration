#!/bin/bash

# Gmail-to-Obsidian Integration - Development Setup Script

set -e

echo "🚀 Gmail-to-Obsidian Integration - Development Setup"
echo ""

# Check Python version
echo "📋 Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
required_version="3.11"

if [[ $(echo -e "$python_version\n$required_version" | sort -V | head -n1) == "$required_version" ]]; then
    echo "✅ Python $python_version (>= $required_version required)"
else
    echo "❌ Python $python_version found, but >= $required_version required"
    exit 1
fi

# Create virtual environment
if [ ! -d "venv" ]; then
    echo ""
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo ""
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo ""
echo "📚 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Dependencies installed"

# Copy .env.example if .env doesn't exist
if [ ! -f ".env" ]; then
    echo ""
    echo "⚙️  Creating .env file from template..."
    cp .env.example .env
    echo "✅ .env file created - IMPORTANT: Edit .env with your actual credentials!"
    echo ""
    echo "Required credentials:"
    echo "  - SUPABASE_URL, SUPABASE_KEY, DATABASE_URL"
    echo "  - GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET"
    echo "  - ANTHROPIC_API_KEY"
    echo "  - SECRET_KEY (generate with: openssl rand -hex 32)"
else
    echo "✅ .env file already exists"
fi

# Check if Redis is installed
echo ""
echo "🔍 Checking for Redis..."
if command -v redis-server &> /dev/null; then
    echo "✅ Redis is installed"
    echo "   Start with: redis-server"
else
    echo "⚠️  Redis not found - install with:"
    echo "   macOS: brew install redis"
    echo "   Ubuntu: sudo apt-get install redis-server"
fi

# Initialize Alembic (if migrations folder is empty)
if [ ! -f "migrations/env.py" ]; then
    echo ""
    echo "🗄️  Initializing Alembic..."
    alembic init migrations
    echo "✅ Alembic initialized"
fi

echo ""
echo "✨ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env with your actual credentials"
echo "2. Start Redis: redis-server"
echo "3. Run migrations: alembic upgrade head"
echo "4. Start FastAPI: uvicorn src.api.main:app --reload"
echo "5. Start Celery: celery -A src.worker.celery_app worker --loglevel=info"
echo "6. (Optional) Start Flower: celery -A src.worker.celery_app flower"
echo ""
echo "Access:"
echo "  - API Docs: http://localhost:8000/docs"
echo "  - Flower: http://localhost:5555"
