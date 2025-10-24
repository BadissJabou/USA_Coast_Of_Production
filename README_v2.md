# Agricultural Data Collection System v2.0

A modernized, production-ready system for collecting, validating, and serving agricultural crop budget data from multiple US state university sources and USDA.

## 🚀 What's New in v2.0

### **Modern Architecture**
- **Base Scraper Class**: Standardized code structure across all scrapers
- **Configuration Management**: YAML-based configuration system
- **Data Validation**: Comprehensive data quality checks and validation
- **Database Integration**: SQLite database for better data management
- **REST API**: FastAPI-based API for data access
- **Testing Framework**: Comprehensive test suite with pytest
- **Pipeline Orchestration**: Automated data collection pipeline

### **Enhanced Features**
- **Rate Limiting**: Respectful web scraping with delays and retry logic
- **Error Handling**: Robust error handling and logging
- **Data Versioning**: Track data changes and maintain history
- **Performance Monitoring**: Health checks and performance metrics
- **Export Capabilities**: Multiple export formats (Excel, CSV, JSON)

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Testing](#testing)
- [Architecture](#architecture)
- [Contributing](#contributing)

## 🛠 Installation

### Prerequisites
- Python 3.8+
- pip package manager

### Install Dependencies

```bash
# Clone the repository
git clone <repository-url>
cd USA_Coast_Of_Production

# Install dependencies
pip install -r requirements.txt
```

### Optional Dependencies
For PDF processing, you may need additional system dependencies:

```bash
# Ubuntu/Debian
sudo apt-get install openjdk-8-jdk

# macOS
brew install openjdk@8

# Windows
# Download and install Java 8 JDK
```

## 🚀 Quick Start

### 1. Run the Pipeline
```bash
# Run all scrapers
python main.py

# Run specific scrapers
python main.py --sources usda iowa

# Check pipeline status
python main.py --status
```

### 2. Start the API Server
```bash
python main.py --api
```

The API will be available at `http://localhost:8000`

### 3. Access API Documentation
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## ⚙️ Configuration

The system uses `config.yaml` for all configuration settings:

```yaml
# Global settings
global:
  project_name: "USA Coast of Production"
  version: "2.0.0"

# Logging configuration
logging:
  level: "INFO"
  file: "logs/scraper.log"

# HTTP request settings
requests:
  timeout: 15
  retries: 3
  delay_min: 1.0
  delay_max: 3.0

# State-specific configurations
states:
  usda:
    enabled: true
    url: "https://www.ers.usda.gov/data-products/commodity-costs-and-returns/"
    commodities: ["corn", "soybeans"]
    output_file: "usda_data.xlsx"
```

### Key Configuration Sections

- **`logging`**: Logging levels, file paths, and formats
- **`requests`**: HTTP request settings, timeouts, retries
- **`data`**: Data validation and output settings
- **`states`**: Individual scraper configurations
- **`database`**: Database settings and table names
- **`api`**: API server configuration

## 📖 Usage

### Command Line Interface

```bash
# Run complete pipeline
python main.py

# Run specific sources
python main.py --sources usda ohio iowa

# Check system status
python main.py --status

# Clean up old data
python main.py --cleanup 30

# Export data
python main.py --export data/export.xlsx --format excel

# Start API server
python main.py --api
```

### Programmatic Usage

```python
from main import AgriculturalDataPipeline

# Initialize pipeline
pipeline = AgriculturalDataPipeline("config.yaml")

# Run scraping
results = await pipeline.run_scraping_pipeline(['usda', 'iowa'])

# Get status
status = pipeline.get_pipeline_status()

# Export data
pipeline.export_data("output.xlsx", "excel")
```

### Individual Scrapers

```python
from scripts.usda_modernized import USDAScraper

# Initialize scraper
scraper = USDAScraper("config.yaml")

# Run scraper
data = scraper.run()

# Validate data
from scripts.data_validator import DataValidator
validator = DataValidator()
result = validator.validate_data(data)
```

## 🌐 API Documentation

### Endpoints

#### **Data Access**
- `GET /data` - Get agricultural data with filters
- `GET /data/{record_id}` - Get specific record by ID
- `POST /data/validate` - Validate data records

#### **Metadata**
- `GET /summary` - Get database summary statistics
- `GET /commodities` - Get available commodities
- `GET /locations` - Get available locations
- `GET /sources` - Get available data sources
- `GET /years` - Get available years

#### **Export**
- `GET /export` - Export data in various formats

#### **System**
- `GET /health` - Health check endpoint

### Example API Calls

```bash
# Get all corn data from Iowa
curl "http://localhost:8000/data?commodity=Corn&location=Iowa"

# Get data summary
curl "http://localhost:8000/summary"

# Export data as Excel
curl "http://localhost:8000/export?format=excel" -o data.xlsx

# Validate data
curl -X POST "http://localhost:8000/data/validate" \
  -H "Content-Type: application/json" \
  -d '[{"item":"Fertilizer","value":150.0,"unit":"$/acre","commodity":"Corn","location":"Iowa","year":"2023/2024","source":"USDA"}]'
```

## 🧪 Testing

### Run Tests
```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=scripts --cov-report=html

# Run specific test file
pytest tests/test_scrapers.py -v
```

### Test Structure
```
tests/
├── test_scrapers.py          # Main test suite
├── test_data_validator.py    # Data validation tests
├── test_database.py          # Database tests
└── test_api.py              # API tests
```

### Test Categories
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end pipeline testing
- **API Tests**: REST API endpoint testing
- **Data Validation Tests**: Data quality and validation testing

## 🏗 Architecture

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Scrapers      │    │   Validator     │    │   Database      │
│                 │    │                 │    │                 │
│ • USDA          │───▶│ • Schema Check  │───▶│ • SQLite        │
│ • Iowa          │    │ • Data Types    │    │ • Raw Data      │
│ • Ohio          │    │ • Range Check   │    │ • Processed     │
│ • North Dakota  │    │ • Format Check  │    │ • Metadata      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Pipeline      │    │   API Server    │    │   Export        │
│                 │    │                 │    │                 │
│ • Orchestration │    │ • FastAPI       │    │ • Excel         │
│ • Scheduling    │    │ • REST Endpoints│    │ • CSV           │
│ • Monitoring    │    │ • Documentation │    │ • JSON          │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

1. **Scraping**: Collect data from various sources
2. **Validation**: Validate data quality and format
3. **Storage**: Store in SQLite database
4. **Processing**: Clean and standardize data
5. **Serving**: Provide via REST API
6. **Export**: Export in various formats

### Database Schema

#### Raw Data Table
```sql
CREATE TABLE raw_scraped_data (
    id INTEGER PRIMARY KEY,
    item TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    commodity TEXT NOT NULL,
    location TEXT NOT NULL,
    year TEXT NOT NULL,
    source TEXT NOT NULL,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_hash TEXT
);
```

#### Processed Data Table
```sql
CREATE TABLE processed_data (
    id INTEGER PRIMARY KEY,
    item TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    commodity TEXT NOT NULL,
    location TEXT NOT NULL,
    year TEXT NOT NULL,
    source TEXT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_version TEXT
);
```

## 📊 Data Sources

### Currently Supported
- **USDA**: National corn and soybeans data (recent, historical, forecast)
- **Iowa State University**: Extension reports via PDF extraction
- **Ohio State University**: Enterprise budgets from XLS files
- **North Dakota State University**: Projected budgets from XLS files

### Planned Support
- **Indiana (Purdue University)**: Crop budgets via PDF extraction
- **Mississippi State University**: Archived budgets in PDF format
- **Tennessee (University of Tennessee)**: Crop budgets from XLSM files

## 🔧 Development

### Code Structure
```
├── scripts/
│   ├── base_scraper.py      # Base scraper class
│   ├── data_validator.py    # Data validation
│   ├── database_manager.py  # Database operations
│   ├── usda_modernized.py  # USDA scraper
│   └── ...                 # Other scrapers
├── api/
│   └── app.py              # FastAPI application
├── tests/
│   └── test_scrapers.py    # Test suite
├── config.yaml             # Configuration
├── main.py                 # Main orchestration
└── requirements.txt        # Dependencies
```

### Adding New Scrapers

1. **Create scraper class**:
```python
from scripts.base_scraper import BaseScraper

class NewStateScraper(BaseScraper):
    def __init__(self, config_path="config.yaml"):
        super().__init__("NewState", config_path)
    
    def scrape_data(self):
        # Implementation here
        pass
```

2. **Add configuration**:
```yaml
states:
  new_state:
    enabled: true
    url: "https://example.com"
    commodities: ["corn", "soybeans"]
```

3. **Add tests**:
```python
def test_new_state_scraper():
    scraper = NewStateScraper()
    data = scraper.run()
    assert data is not None
```

### Code Quality

```bash
# Format code
black scripts/ tests/

# Sort imports
isort scripts/ tests/

# Type checking
mypy scripts/

# Linting
flake8 scripts/ tests/
```

## 🚀 Deployment

### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "main.py", "--api"]
```

### Production Considerations
- Configure proper logging levels
- Set up database backups
- Implement monitoring and alerting
- Use environment variables for sensitive data
- Configure rate limiting and security headers

## 📈 Performance

### Optimization Features
- **Parallel Processing**: Concurrent scraping when possible
- **Caching**: Intelligent caching of requests
- **Database Indexing**: Optimized database queries
- **Rate Limiting**: Respectful web scraping
- **Connection Pooling**: Efficient HTTP connections

### Monitoring
- Health check endpoints
- Performance metrics
- Error tracking
- Data quality scores

## 🤝 Contributing

### Getting Started
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

### Guidelines
- Follow PEP 8 style guidelines
- Add comprehensive tests
- Update documentation
- Use meaningful commit messages
- Ensure all tests pass

### Development Setup
```bash
# Clone your fork
git clone <your-fork-url>
cd USA_Coast_Of_Production

# Install development dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Start development server
python main.py --api
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/your-repo/issues)
- **Documentation**: [API Docs](http://localhost:8000/docs)
- **Email**: your-email@example.com

## 🙏 Acknowledgments

- USDA Economic Research Service for providing agricultural data
- State university extension services for their research and data
- Open source community for the tools and libraries used

---

**Version**: 2.0.0  
**Last Updated**: 2024  
**Python Version**: 3.8+
