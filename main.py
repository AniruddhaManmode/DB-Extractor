import json
import os
import sqlite3
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path

# Database drivers (optional - install as needed)
try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import pyodbc
    SQLSERVER_AVAILABLE = True
except ImportError:
    SQLSERVER_AVAILABLE = False


# Configuration storage path
CONFIG_DIR = Path.home() / ".db_extractor"
CONFIG_FILE = CONFIG_DIR / "db_configs.json"


@dataclass
class DBConfig:
    """Database configuration dataclass"""
    name: str  # Config identifier
    db_type: str  # mysql, postgres, sqlite, sqlserver
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""
    # For SQLite
    file_path: str = ""
    # Additional options
    options: Dict[str, Any] = None

    def __post_init__(self):
        if self.options is None:
            self.options = {}

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'DBConfig':
        return cls(**data)


class ConfigManager:
    """Manages database configuration files"""

    def __init__(self):
        self._ensure_config_dir()

    def _ensure_config_dir(self):
        """Create config directory if not exists"""
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    def save_config(self, config: DBConfig) -> bool:
        """Save a database configuration"""
        try:
            configs = self.load_all_configs()
            configs[config.name] = config.to_dict()

            with open(CONFIG_FILE, 'w') as f:
                json.dump(configs, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False

    def load_all_configs(self) -> Dict:
        """Load all saved configurations"""
        if not CONFIG_FILE.exists():
            return {}

        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading configs: {e}")
            return {}

    def load_config(self, name: str) -> Optional[DBConfig]:
        """Load a specific configuration by name"""
        configs = self.load_all_configs()
        if name in configs:
            return DBConfig.from_dict(configs[name])
        return None

    def delete_config(self, name: str) -> bool:
        """Delete a configuration"""
        try:
            configs = self.load_all_configs()
            if name in configs:
                del configs[name]
                with open(CONFIG_FILE, 'w') as f:
                    json.dump(configs, f, indent=2)
                return True
            return False
        except Exception as e:
            print(f"Error deleting config: {e}")
            return False

    def list_configs(self) -> List[str]:
        """List all saved configuration names"""
        return list(self.load_all_configs().keys())


class SchemaExtractor:
    """Extracts database structure/schema"""

    def __init__(self, config: DBConfig):
        self.config = config
        self.connection = None

    def connect(self) -> bool:
        """Establish database connection"""
        try:
            if self.config.db_type == "sqlite":
                self.connection = sqlite3.connect(self.config.file_path)
            elif self.config.db_type == "mysql":
                if not MYSQL_AVAILABLE:
                    print("MySQL driver not installed. Run: pip install pymysql")
                    return False
                self.connection = pymysql.connect(
                    host=self.config.host,
                    port=self.config.port or 3306,
                    user=self.config.username,
                    password=self.config.password,
                    database=self.config.database,
                    cursorclass=pymysql.cursors.DictCursor
                )
            elif self.config.db_type == "postgres":
                if not POSTGRES_AVAILABLE:
                    print("PostgreSQL driver not installed. Run: pip install psycopg2")
                    return False
                self.connection = psycopg2.connect(
                    host=self.config.host,
                    port=self.config.port or 5432,
                    user=self.config.username,
                    password=self.config.password,
                    dbname=self.config.database
                )
            elif self.config.db_type == "sqlserver":
                if not SQLSERVER_AVAILABLE:
                    print("SQL Server driver not installed. Run: pip install pyodbc")
                    return False
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.config.host},{self.config.port or 1433};"
                    f"DATABASE={self.config.database};"
                    f"UID={self.config.username};"
                    f"PWD={self.config.password}"
                )
                self.connection = pyodbc.connect(conn_str)
            else:
                print(f"Unsupported database type: {self.config.db_type}")
                return False
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def extract_schema(self) -> Dict:
        """Extract complete database schema"""
        if not self.connection and not self.connect():
            return {}

        schema = {
            "database_name": self.config.database or self.config.file_path,
            "db_type": self.config.db_type,
            "tables": []
        }

        try:
            if self.config.db_type == "sqlite":
                schema["tables"] = self._extract_sqlite_schema()
            elif self.config.db_type == "mysql":
                schema["tables"] = self._extract_mysql_schema()
            elif self.config.db_type == "postgres":
                schema["tables"] = self._extract_postgres_schema()
            elif self.config.db_type == "sqlserver":
                schema["tables"] = self._extract_sqlserver_schema()
        except Exception as e:
            print(f"Schema extraction error: {e}")

        return schema

    def _extract_sqlite_schema(self) -> List[Dict]:
        """Extract SQLite schema"""
        tables = []
        cursor = self.connection.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        table_names = [row[0] for row in cursor.fetchall()]

        for table_name in table_names:
            # Get table info
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[1],
                    "type": row[2],
                    "nullable": not row[3],
                    "default": row[4],
                    "primary_key": bool(row[5])
                })

            # Get foreign keys
            cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
            foreign_keys = []
            for row in cursor.fetchall():
                foreign_keys.append({
                    "column": row[3],
                    "references_table": row[2],
                    "references_column": row[4]
                })

            # Get indexes
            cursor.execute(f'PRAGMA index_list("{table_name}")')
            indexes = []
            for row in cursor.fetchall():
                index_name = row[1]
                cursor.execute(f'PRAGMA index_info("{index_name}")')
                index_columns = [r[2] for r in cursor.fetchall()]
                indexes.append({
                    "name": index_name,
                    "unique": bool(row[2]),
                    "columns": index_columns
                })

            tables.append({
                "name": table_name,
                "columns": columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            })

        return tables

    def _extract_mysql_schema(self) -> List[Dict]:
        """Extract MySQL schema"""
        tables = []
        cursor = self.connection.cursor()

        cursor.execute("SHOW TABLES")
        table_names = [row[f"Tables_in_{self.config.database}"] for row in cursor.fetchall()]

        for table_name in table_names:
            # Get columns
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row["Field"],
                    "type": row["Type"],
                    "nullable": row["Null"] == "YES",
                    "default": row["Default"],
                    "primary_key": row["Key"] == "PRI",
                    "extra": row["Extra"]
                })

            # Get foreign keys
            cursor.execute(f"""
                SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (self.config.database, table_name))
            foreign_keys = []
            for row in cursor.fetchall():
                foreign_keys.append({
                    "column": row["COLUMN_NAME"],
                    "references_table": row["REFERENCED_TABLE_NAME"],
                    "references_column": row["REFERENCED_COLUMN_NAME"]
                })

            # Get indexes
            cursor.execute(f"SHOW INDEX FROM `{table_name}`")
            indexes = []
            index_groups = {}
            for row in cursor.fetchall():
                idx_name = row["Key_name"]
                if idx_name not in index_groups:
                    index_groups[idx_name] = {
                        "name": idx_name,
                        "unique": not row["Non_unique"],
                        "columns": []
                    }
                index_groups[idx_name]["columns"].append(row["Column_name"])
            indexes = list(index_groups.values())

            tables.append({
                "name": table_name,
                "columns": columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            })

        return tables

    def _extract_postgres_schema(self) -> List[Dict]:
        """Extract PostgreSQL schema"""
        tables = []
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' ORDER BY table_name
        """)
        table_names = [row[0] for row in cursor.fetchall()]

        for table_name in table_names:
            # Get columns
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3]
                })

            # Get primary keys
            cursor.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
            """, (table_name,))
            pk_columns = [row[0] for row in cursor.fetchall()]
            for col in columns:
                col["primary_key"] = col["name"] in pk_columns

            # Get foreign keys
            cursor.execute("""
                SELECT kcu.column_name, ccu.table_name AS references_table, ccu.column_name AS references_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.table_name = %s AND tc.constraint_type = 'FOREIGN KEY'
            """, (table_name,))
            foreign_keys = []
            for row in cursor.fetchall():
                foreign_keys.append({
                    "column": row[0],
                    "references_table": row[1],
                    "references_column": row[2]
                })

            # Get indexes
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s AND schemaname = 'public'
            """, (table_name,))
            indexes = []
            for row in cursor.fetchall():
                # Parse indexdef to extract unique status and columns
                indexdef = row[1]
                unique = "UNIQUE" in indexdef.upper()
                # Simple parsing - extract column names
                columns_part = indexdef.split("(")[1].split(")")[0] if "(" in indexdef else ""
                index_columns = [c.strip() for c in columns_part.split(",") if c.strip()]
                indexes.append({
                    "name": row[0],
                    "unique": unique,
                    "columns": index_columns
                })

            tables.append({
                "name": table_name,
                "columns": columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            })

        return tables

    def _extract_sqlserver_schema(self) -> List[Dict]:
        """Extract SQL Server schema"""
        tables = []
        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME
        """)
        table_names = [row[0] for row in cursor.fetchall()]

        for table_name in table_names:
            # Get columns
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, (table_name,))
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3]
                })

            # Get primary keys
            cursor.execute("""
                SELECT kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """, (table_name,))
            pk_columns = [row[0] for row in cursor.fetchall()]
            for col in columns:
                col["primary_key"] = col["name"] in pk_columns

            # Get foreign keys
            cursor.execute("""
                SELECT kcu.COLUMN_NAME, ccu.TABLE_NAME AS references_table, ccu.COLUMN_NAME AS references_column
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
                    ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                WHERE tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            """, (table_name,))
            foreign_keys = []
            for row in cursor.fetchall():
                foreign_keys.append({
                    "column": row[0],
                    "references_table": row[1],
                    "references_column": row[2]
                })

            # Get indexes
            cursor.execute("""
                SELECT i.name, i.is_unique, c.name AS column_name
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.tables t ON i.object_id = t.object_id
                WHERE t.name = ? AND i.type > 0
                ORDER BY i.name, ic.key_ordinal
            """, (table_name,))
            index_groups = {}
            for row in cursor.fetchall():
                idx_name = row[0]
                if idx_name not in index_groups:
                    index_groups[idx_name] = {
                        "name": idx_name,
                        "unique": bool(row[1]),
                        "columns": []
                    }
                index_groups[idx_name]["columns"].append(row[2])
            indexes = list(index_groups.values())

            tables.append({
                "name": table_name,
                "columns": columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            })

        return tables

    def save_schema_to_file(self, schema: Dict, output_path: str):
        """Save extracted schema to JSON file"""
        with open(output_path, 'w') as f:
            json.dump(schema, f, indent=2)
        print(f"Schema saved to: {output_path}")

    def generate_sql_ddl(self, schema: Dict) -> str:
        """Generate SQL DDL from schema"""
        ddl = []
        ddl.append(f"-- Database: {schema['database_name']}")
        ddl.append(f"-- Type: {schema['db_type']}")
        ddl.append("")

        for table in schema["tables"]:
            ddl.append(f"CREATE TABLE {table['name']} (")
            col_defs = []
            for col in table["columns"]:
                col_def = f"    {col['name']} {col['type']}"
                if col.get("primary_key") and schema["db_type"] == "sqlite":
                    col_def += " PRIMARY KEY"
                if not col.get("nullable", True):
                    col_def += " NOT NULL"
                if col.get("default") is not None:
                    col_def += f" DEFAULT {col['default']}"
                col_defs.append(col_def)

            # Add primary key constraint for non-SQLite
            pk_cols = [c["name"] for c in table["columns"] if c.get("primary_key")]
            if pk_cols and schema["db_type"] != "sqlite":
                col_defs.append(f"    PRIMARY KEY ({', '.join(pk_cols)})")

            ddl.append(",\n".join(col_defs))
            ddl.append(");")
            ddl.append("")

            # Add foreign keys
            for fk in table.get("foreign_keys", []):
                ddl.append(f"ALTER TABLE {table['name']} ADD CONSTRAINT fk_{table['name']}_{fk['column']}")
                ddl.append(f"    FOREIGN KEY ({fk['column']}) REFERENCES {fk['references_table']}({fk['references_column']});")
                ddl.append("")

            # Add indexes
            for idx in table.get("indexes", []):
                if idx["name"] == "PRIMARY":
                    continue
                unique = "UNIQUE " if idx.get("unique") else ""
                ddl.append(f"CREATE {unique}INDEX {idx['name']} ON {table['name']} ({', '.join(idx['columns'])});")

            ddl.append("")

        return "\n".join(ddl)


class InteractiveCLI:
    """Interactive command-line interface"""

    def __init__(self):
        self.config_manager = ConfigManager()

    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')

    def print_header(self, title: str):
        """Print formatted header"""
        print("\n" + "=" * 60)
        print(f"  {title}")
        print("=" * 60 + "\n")

    def main_menu(self):
        """Display main menu"""
        while True:
            self.clear_screen()
            self.print_header("DATABASE STRUCTURE EXTRACTOR")
            print("1. Manage Database Configurations")
            print("2. Extract Database Schema")
            print("3. View Saved Configurations")
            print("4. Exit")
            print()

            choice = input("Select option (1-4): ").strip()

            if choice == "1":
                self.config_menu()
            elif choice == "2":
                self.extract_menu()
            elif choice == "3":
                self.view_configs()
            elif choice == "4":
                print("\nGoodbye!")
                break
            else:
                input("Invalid choice. Press Enter to continue...")

    def config_menu(self):
        """Database configuration management menu"""
        while True:
            self.clear_screen()
            self.print_header("DATABASE CONFIGURATION MANAGEMENT")
            print("1. Add New Configuration")
            print("2. Edit Configuration")
            print("3. Delete Configuration")
            print("4. Back to Main Menu")
            print()

            choice = input("Select option (1-4): ").strip()

            if choice == "1":
                self.add_config()
            elif choice == "2":
                self.edit_config()
            elif choice == "3":
                self.delete_config()
            elif choice == "4":
                break
            else:
                input("Invalid choice. Press Enter to continue...")

    def add_config(self):
        """Add new database configuration"""
        self.clear_screen()
        self.print_header("ADD NEW DATABASE CONFIGURATION")

        print("Database Types:")
        print("  1. MySQL")
        print("  2. PostgreSQL")
        print("  3. SQLite")
        print("  4. SQL Server")
        print()

        db_type_choice = input("Select database type (1-4): ").strip()
        db_types = {"1": "mysql", "2": "postgres", "3": "sqlite", "4": "sqlserver"}

        if db_type_choice not in db_types:
            input("Invalid choice. Press Enter to continue...")
            return

        db_type = db_types[db_type_choice]

        config_name = input("Configuration name (e.g., 'production-db'): ").strip()
        if not config_name:
            input("Name is required. Press Enter to continue...")
            return

        # Check if name already exists
        if self.config_manager.load_config(config_name):
            overwrite = input(f"Config '{config_name}' exists. Overwrite? (y/n): ").strip().lower()
            if overwrite != 'y':
                return

        if db_type == "sqlite":
            file_path = input("Database file path (e.g., /path/to/database.db): ").strip()
            config = DBConfig(
                name=config_name,
                db_type=db_type,
                file_path=file_path
            )
        else:
            host = input("Host (e.g., localhost or 127.0.0.1): ").strip() or "localhost"
            port = input(f"Port (default: {3306 if db_type == 'mysql' else 5432 if db_type == 'postgres' else 1433}): ").strip()
            port = int(port) if port.isdigit() else 0
            database = input("Database name: ").strip()
            username = input("Username: ").strip()
            password = input("Password: ").strip()

            config = DBConfig(
                name=config_name,
                db_type=db_type,
                host=host,
                port=port,
                database=database,
                username=username,
                password=password
            )

        if self.config_manager.save_config(config):
            print(f"\n✓ Configuration '{config_name}' saved successfully!")
            print(f"  Location: {CONFIG_FILE}")
        else:
            print("\n✗ Failed to save configuration.")

        input("\nPress Enter to continue...")

    def edit_config(self):
        """Edit existing configuration"""
        self.clear_screen()
        self.print_header("EDIT CONFIGURATION")

        configs = self.config_manager.list_configs()
        if not configs:
            print("No configurations found.")
            input("Press Enter to continue...")
            return

        print("Available configurations:")
        for i, name in enumerate(configs, 1):
            print(f"  {i}. {name}")
        print()

        choice = input("Enter configuration number or name: ").strip()

        config_name = None
        if choice.isdigit() and 1 <= int(choice) <= len(configs):
            config_name = configs[int(choice) - 1]
        elif choice in configs:
            config_name = choice

        if not config_name:
            input("Invalid selection. Press Enter to continue...")
            return

        # Re-use add logic but with existing data
        self.add_config()

    def delete_config(self):
        """Delete configuration"""
        self.clear_screen()
        self.print_header("DELETE CONFIGURATION")

        configs = self.config_manager.list_configs()
        if not configs:
            print("No configurations found.")
            input("Press Enter to continue...")
            return

        print("Available configurations:")
        for i, name in enumerate(configs, 1):
            print(f"  {i}. {name}")
        print()

        choice = input("Enter configuration number or name to delete: ").strip()

        config_name = None
        if choice.isdigit() and 1 <= int(choice) <= len(configs):
            config_name = configs[int(choice) - 1]
        elif choice in configs:
            config_name = choice

        if config_name:
            confirm = input(f"Are you sure you want to delete '{config_name}'? (y/n): ").strip().lower()
            if confirm == 'y':
                if self.config_manager.delete_config(config_name):
                    print(f"\n✓ Configuration '{config_name}' deleted.")
                else:
                    print("\n✗ Failed to delete configuration.")
        else:
            print("Invalid selection.")

        input("\nPress Enter to continue...")

    def view_configs(self):
        """View all saved configurations"""
        self.clear_screen()
        self.print_header("SAVED CONFIGURATIONS")

        configs = self.config_manager.load_all_configs()
        if not configs:
            print("No configurations found.")
            print(f"\nConfig file location: {CONFIG_FILE}")
        else:
            print(f"Total configurations: {len(configs)}")
            print(f"Config file location: {CONFIG_FILE}\n")

            for name, data in configs.items():
                print(f"  [{name}]")
                print(f"    Type: {data['db_type']}")
                if data['db_type'] == 'sqlite':
                    print(f"    File: {data.get('file_path', 'N/A')}")
                else:
                    print(f"    Host: {data.get('host', 'N/A')}:{data.get('port', 'N/A')}")
                    print(f"    Database: {data.get('database', 'N/A')}")
                    print(f"    User: {data.get('username', 'N/A')}")
                print()

        input("\nPress Enter to continue...")

    def extract_menu(self):
        """Schema extraction menu"""
        self.clear_screen()
        self.print_header("EXTRACT DATABASE SCHEMA")

        configs = self.config_manager.list_configs()
        if not configs:
            print("No configurations found. Please add a configuration first.")
            input("\nPress Enter to continue...")
            return

        print("Select configuration:")
        for i, name in enumerate(configs, 1):
            config_data = self.config_manager.load_config(name)
            db_type = config_data.db_type if config_data else "unknown"
            print(f"  {i}. {name} ({db_type})")
        print()

        choice = input("Enter configuration number or name: ").strip()

        config_name = None
        if choice.isdigit() and 1 <= int(choice) <= len(configs):
            config_name = configs[int(choice) - 1]
        elif choice in configs:
            config_name = choice

        if not config_name:
            input("Invalid selection. Press Enter to continue...")
            return

        config = self.config_manager.load_config(config_name)
        if not config:
            input("Failed to load configuration. Press Enter to continue...")
            return

        print(f"\nConnecting to '{config_name}'...")
        extractor = SchemaExtractor(config)

        print("Extracting schema...")
        schema = extractor.extract_schema()

        if not schema or not schema.get("tables"):
            print("No tables found or failed to extract schema.")
            input("\nPress Enter to continue...")
            return

        print(f"\n✓ Found {len(schema['tables'])} tables")

        # Display summary
        print("\nSchema Summary:")
        for table in schema["tables"]:
            print(f"  - {table['name']}: {len(table['columns'])} columns, {len(table.get('foreign_keys', []))} FKs, {len(table.get('indexes', []))} indexes")

        # Save options
        print("\nSave options:")
        print("1. Save as JSON")
        print("2. Save as SQL DDL")
        print("3. Save both")
        print("4. View only (don't save)")
        print()

        save_choice = input("Select option (1-4): ").strip()

        if save_choice in ["1", "2", "3"]:
            output_dir = input("Output directory (default: current): ").strip() or "."
            os.makedirs(output_dir, exist_ok=True)

            if save_choice in ["1", "3"]:
                json_path = os.path.join(output_dir, f"{config_name}_schema.json")
                extractor.save_schema_to_file(schema, json_path)

            if save_choice in ["2", "3"]:
                sql_path = os.path.join(output_dir, f"{config_name}_schema.sql")
                ddl = extractor.generate_sql_ddl(schema)
                with open(sql_path, 'w') as f:
                    f.write(ddl)
                print(f"DDL saved to: {sql_path}")

            if save_choice == "4":
                print("\nJSON Schema:")
                print(json.dumps(schema, indent=2)[:1000] + "..." if len(json.dumps(schema)) > 1000 else json.dumps(schema, indent=2))

        extractor.disconnect()
        input("\nPress Enter to continue...")


def main():
    """Main entry point"""
    cli = InteractiveCLI()
    cli.main_menu()


if __name__ == "__main__":
    main()
