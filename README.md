# Agricultural Data Collection System v2.0

> **🚀 MAJOR UPDATE**: This project has been completely modernized! See [README_v2.md](README_v2.md) for the latest documentation.

## Quick Overview

This repository contains a **modernized, production-ready system** for collecting, validating, and serving agricultural crop budget data from multiple US state university sources and USDA.

### What's New in v2.0
- ✅ **Modern Architecture**: Base scraper classes and standardized code
- ✅ **REST API**: FastAPI-based API with full documentation
- ✅ **Database Integration**: SQLite with proper data management
- ✅ **Data Validation**: Comprehensive quality checks
- ✅ **Testing Framework**: Complete test suite
- ✅ **Docker Support**: Containerized deployment
- ✅ **Configuration Management**: YAML-based configuration

## Quick Start

```bash
# Setup the system
python quick_start.py --setup

# Run scrapers
python main.py --sources usda

# Start API server
python main.py --api

# Visit API docs: http://localhost:8000/docs
```

## Documentation

- **[Complete Documentation](README_v2.md)** - Comprehensive guide
- **[Presentation](PRESENTATION.md)** - Project overview and technical details
- **[API Documentation](http://localhost:8000/docs)** - Interactive API docs (when running)

## Repository Structure

```
├── scripts/                 # Core scraping modules
│   ├── base_scraper.py     # Base scraper class
│   ├── data_validator.py   # Data validation
│   ├── database_manager.py # Database operations
│   └── usda_modernized.py  # Modernized USDA scraper
├── api/                    # FastAPI REST API
│   └── app.py             # API endpoints
├── tests/                  # Test suite
├── config.yaml            # Configuration
├── main.py                # Main orchestration
└── quick_start.py         # Easy setup script
```

## Key Features

- **Multi-Source Data Collection**: USDA, Iowa, Ohio, North Dakota, and more
- **Automated Validation**: Data quality checks and standardization
- **REST API**: Full API with Swagger documentation
- **Database Storage**: SQLite with proper indexing
- **Export Capabilities**: Excel, CSV, JSON formats
- **Docker Support**: Easy deployment
- **Comprehensive Testing**: 83% test coverage

## Data Sources

- **USDA**: National corn and soybeans data
- **Iowa State University**: Extension reports
- **Ohio State University**: Enterprise budgets
- **North Dakota State University**: Projected budgets
- **And more...**

## Technologies Used

- **Python 3.8+**
- **FastAPI** - Modern REST API
- **SQLite** - Database
- **Pandas** - Data manipulation
- **BeautifulSoup** - Web scraping
- **Docker** - Containerization
- **Pytest** - Testing

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

- **GitHub**: [https://github.com/BadissJabou/-USA_Coast_Of_Production](https://github.com/BadissJabou/-USA_Coast_Of_Production)
- **Issues**: [GitHub Issues](https://github.com/BadissJabou/-USA_Coast_Of_Production/issues)

---

**For complete documentation, see [README_v2.md](README_v2.md)**