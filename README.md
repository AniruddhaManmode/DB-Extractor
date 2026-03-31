# Database Structure Extractor

A comprehensive Python tool for extracting, documenting, and generating DDL from database schemas. Supports multiple database systems including MySQL, PostgreSQL, SQLite, and SQL Server.

## Features

- 🔌 **Multi-database support**: MySQL, PostgreSQL, SQLite, SQL Server
- 📊 **Schema extraction**: Extract complete database structure including tables, columns, data types, constraints, and indexes
- 💾 **Multiple output formats**: JSON schema export and SQL DDL generation
- 🗄️ **Configuration management**: Save and manage multiple database connections
- 🔍 **Interactive CLI**: User-friendly command-line interface
- 📝 **Foreign key detection**: Automatically identifies and documents relationships
- 🔑 **Primary key detection**: Identifies primary keys across different database systems
- 📑 **Index extraction**: Captures all indexes including unique constraints

## Installation

### Prerequisites
- Python 3.7 or higher
- pip package manager

### Install from source (linux)

1. Clone the repository:
```bash
python3 -m venv .venv
source .venv/bin/activate
git clone https://github.com/AniruddhaManmode/DB-Extractor.git
cd DB-Extractor
