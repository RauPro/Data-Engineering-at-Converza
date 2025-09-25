#!/usr/bin/env python3
"""
Test script for Converza Pipeline components
Verifies that all components work correctly before full deployment
"""

import os
import sys
import json
from pathlib import Path

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_data_generation():
    """Test synthetic data generation"""
    print("🧪 Testing data generation...")
    
    try:
        from src.data_generator import CallDataGenerator
        
        generator = CallDataGenerator()
        
        # Generate a single call for testing
        call = generator.generate_call_record()
        
        # Validate structure
        required_fields = ['call_id', 'agent_name', 'timestamp', 'transcript_text']
        for field in required_fields:
            assert field in call, f"Missing field: {field}"
        
        print(f"   ✅ Generated call {call['call_id']} by {call['agent_name']}")
        return True
        
    except Exception as e:
        print(f"   ❌ Data generation failed: {e}")
        return False

def test_etl_processing():
    """Test ETL processing logic"""
    print("🧪 Testing ETL processing...")
    
    try:
        from src.etl_processors import CallTranscriptProcessor
        
        processor = CallTranscriptProcessor()
        
        # Test transcript with known patterns
        test_transcript = """
        Agent: Hello, thank you for calling. How can I help you today?
        Customer: I'm looking into your services.
        Agent: Great! So would you like to proceed?
        Customer: Yes, I'll book that appointment. This is fantastic.
        Agent: Excellent! I also have some additional options.
        Customer: I'll take the premium package for an extra $50.
        """
        
        test_call = {
            'call_id': 'TEST_001',
            'agent_name': 'Test Agent',
            'timestamp': '2025-01-01T12:00:00',
            'transcript_text': test_transcript
        }
        
        # Process the call
        kpis = processor.extract_all_kpis(test_call)
        
        # Validate results
        assert kpis['is_conversion'] == True, "Should detect conversion"
        assert kpis['upsell_amount'] == 50.0, f"Should detect $50 upsell, got ${kpis['upsell_amount']}"
        assert kpis['sentiment_score'] == 'Positive', f"Should detect positive sentiment, got {kpis['sentiment_score']}"
        
        print(f"   ✅ ETL processing successful:")
        print(f"      - Conversion: {kpis['is_conversion']}")
        print(f"      - Upsell: ${kpis['upsell_amount']}")
        print(f"      - Sentiment: {kpis['sentiment_score']}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ ETL processing failed: {e}")
        return False

def test_dashboard_import():
    """Test dashboard imports"""
    print("🧪 Testing dashboard imports...")
    
    try:
        # Test if all required packages are available
        import dash
        import plotly.express as px
        import pandas as pd
        
        print("   ✅ Dashboard dependencies available")
        return True
        
    except ImportError as e:
        print(f"   ❌ Dashboard import failed: {e}")
        print("   💡 Run 'uv add dash plotly pandas' to install missing packages")
        return False

def test_configuration():
    """Test configuration files"""
    print("🧪 Testing configuration...")
    
    try:
        from config.settings import DATABASE_CONFIG, MINIO_CONFIG, DASHBOARD_CONFIG
        
        # Validate configuration structure
        assert 'host' in DATABASE_CONFIG, "Database config missing host"
        assert 'endpoint' in MINIO_CONFIG, "MinIO config missing endpoint"
        assert 'port' in DASHBOARD_CONFIG, "Dashboard config missing port"
        
        print("   ✅ Configuration files valid")
        return True
        
    except Exception as e:
        print(f"   ❌ Configuration test failed: {e}")
        return False

def test_docker_files():
    """Test Docker configuration files exist"""
    print("🧪 Testing Docker configuration...")
    
    required_files = [
        'docker/docker-compose.yml',
        'docker/init.sql',
        'docker/init-data.sql',
        'docker/superset_config.py'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"   ❌ Missing Docker files: {missing_files}")
        return False
    
    print("   ✅ All Docker configuration files present")
    return True

def test_directory_structure():
    """Test required directories exist"""
    print("🧪 Testing directory structure...")
    
    required_dirs = [
        'src',
        'dags',
        'dashboard',
        'config',
        'docker',
        'data',
        'data/raw_calls',
        'data/processed'
    ]
    
    missing_dirs = []
    for dir_path in required_dirs:
        if not Path(dir_path).exists():
            missing_dirs.append(dir_path)
    
    if missing_dirs:
        print(f"   ❌ Missing directories: {missing_dirs}")
        return False
    
    print("   ✅ All required directories present")
    return True

def main():
    """Run all tests"""
    print("🚀 Running Converza Pipeline Tests...")
    print("=" * 50)
    
    tests = [
        test_directory_structure,
        test_configuration,
        test_docker_files,
        test_data_generation,
        test_etl_processing,
        test_dashboard_import
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        if test():
            passed += 1
        else:
            failed += 1
        print()
    
    print("=" * 50)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! The pipeline is ready to run.")
        print("💡 Next step: Run './start.sh' to start the full pipeline")
    else:
        print("⚠️  Some tests failed. Please fix the issues before proceeding.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())