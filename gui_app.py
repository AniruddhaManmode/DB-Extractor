import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import threading

from main import DBConfig, ConfigManager, SchemaExtractor, CONFIG_DIR, CONFIG_FILE

APP_DB_PATH = CONFIG_DIR / "app_data.db"
SCHEMA_OUTPUT_DIR = CONFIG_DIR / "schemas"

# --- THEME TOKENS ---
THEME = {
    "light": {
        "bg": "#F8FAFC",
        "surface": "#FFFFFF",
        "sidebar": "#F1F5F9",
        "border": "#E2E8F0",
        "text_primary": "#0F172A",
        "text_secondary": "#64748B",
        "primary": "#2563EB",
        "primary_hover": "#1D4ED8",
        "success": "#10B981",
        "success_hover": "#059669",
        "danger": "#EF4444",
        "danger_hover": "#DC2626",
        "warning": "#F59E0B",
        "tree_bg": "#FFFFFF",
        "tree_fg": "#0F172A",
        "tree_sel_bg": "#E0F2FE",
        "tree_sel_fg": "#0369A1",
    },
    "dark": {
        "bg": "#0F172A",
        "surface": "#111827",
        "sidebar": "#020617",
        "border": "#1E293B",
        "text_primary": "#F8FAFC",
        "text_secondary": "#94A3B8",
        "primary": "#3B82F6",
        "primary_hover": "#2563EB",
        "success": "#10B981",
        "success_hover": "#059669",
        "danger": "#EF4444",
        "danger_hover": "#DC2626",
        "warning": "#F59E0B",
        "tree_bg": "#111827",
        "tree_fg": "#F8FAFC",
        "tree_sel_bg": "#1E3A8A",
        "tree_sel_fg": "#BFDBFE",
    }
}

C_BG = (THEME["light"]["bg"], THEME["dark"]["bg"])
C_SURFACE = (THEME["light"]["surface"], THEME["dark"]["surface"])
C_SIDEBAR = (THEME["light"]["sidebar"], THEME["dark"]["sidebar"])
C_BORDER = (THEME["light"]["border"], THEME["dark"]["border"])
C_TEXT_PRIMARY = (THEME["light"]["text_primary"], THEME["dark"]["text_primary"])
C_TEXT_SECONDARY = (THEME["light"]["text_secondary"], THEME["dark"]["text_secondary"])
C_PRIMARY = (THEME["light"]["primary"], THEME["dark"]["primary"])
C_PRIMARY_HOVER = (THEME["light"]["primary_hover"], THEME["dark"]["primary_hover"])
C_SUCCESS = (THEME["light"]["success"], THEME["dark"]["success"])
C_SUCCESS_HOVER = (THEME["light"]["success_hover"], THEME["dark"]["success_hover"])
C_DANGER = (THEME["light"]["danger"], THEME["dark"]["danger"])
C_DANGER_HOVER = (THEME["light"]["danger_hover"], THEME["dark"]["danger_hover"])

DB_COLORS = {
    "mysql": ("#DBEAFE", "#1E3A8A", "#1D4ED8", "#93C5FD"),
    "postgres": ("#F3E8FF", "#4C1D95", "#6D28D9", "#C4B5FD"),
    "sqlite": ("#F1F5F9", "#334155", "#475569", "#CBD5E1"),
    "sqlserver": ("#FFEDD5", "#7C2D12", "#C2410C", "#FDBA74")
}

def get_badge_color(db_type):
    db = db_type.lower()
    if db in DB_COLORS:
        c = DB_COLORS[db]
        return (c[0], c[1]), (c[2], c[3])
    return C_BORDER, C_TEXT_PRIMARY

# --- APP DB ---
class AppDB:
    def __init__(self):
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        SCHEMA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(APP_DB_PATH), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        self._migrate_json_to_db()

    def _create_tables(self):
        c = self.conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                db_type TEXT NOT NULL,
                host TEXT DEFAULT '',
                port INTEGER DEFAULT 0,
                database TEXT DEFAULT '',
                username TEXT DEFAULT '',
                password TEXT DEFAULT '',
                file_path TEXT DEFAULT '',
                options TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS extractions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id INTEGER NOT NULL,
                config_name TEXT NOT NULL,
                status TEXT NOT NULL,
                tables_found INTEGER DEFAULT 0,
                schema_json TEXT,
                schema_sql TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (config_id) REFERENCES configs(id) ON DELETE CASCADE
            )
        """)
        self.conn.commit()

    def _migrate_json_to_db(self):
        if not CONFIG_FILE.exists():
            return
        try:
            with open(CONFIG_FILE, 'r') as f:
                configs = json.load(f)
            c = self.conn.cursor()
            for name, data in configs.items():
                c.execute("SELECT id FROM configs WHERE name=?", (name,))
                if c.fetchone():
                    continue
                now = datetime.now().isoformat()
                c.execute("""
                    INSERT INTO configs (name, db_type, host, port, database, username, password, file_path, options, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, data.get('db_type', ''), data.get('host', ''), data.get('port', 0),
                      data.get('database', ''), data.get('username', ''), data.get('password', ''),
                      data.get('file_path', ''), json.dumps(data.get('options', {})), now, now))
            self.conn.commit()
        except Exception:
            pass

    def get_all_configs(self) -> List[Dict]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM configs ORDER BY updated_at DESC")
        return [dict(row) for row in c.fetchall()]

    def search_configs(self, query: str) -> List[Dict]:
        c = self.conn.cursor()
        q = f"%{query}%"
        c.execute("SELECT * FROM configs WHERE name LIKE ? OR db_type LIKE ? OR database LIKE ? OR host LIKE ? ORDER BY updated_at DESC",
                  (q, q, q, q))
        return [dict(row) for row in c.fetchall()]

    def get_config(self, config_id: int) -> Optional[Dict]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM configs WHERE id=?", (config_id,))
        row = c.fetchone()
        return dict(row) if row else None

    def save_config(self, data: Dict) -> int:
        now = datetime.now().isoformat()
        c = self.conn.cursor()
        if data.get('id'):
            c.execute("""
                UPDATE configs SET name=?, db_type=?, host=?, port=?, database=?, username=?,
                password=?, file_path=?, options=?, updated_at=? WHERE id=?
            """, (data['name'], data['db_type'], data.get('host', ''), data.get('port', 0),
                  data.get('database', ''), data.get('username', ''), data.get('password', ''),
                  data.get('file_path', ''), json.dumps(data.get('options', {})), now, data['id']))
        else:
            c.execute("""
                INSERT INTO configs (name, db_type, host, port, database, username, password, file_path, options, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (data['name'], data['db_type'], data.get('host', ''), data.get('port', 0),
                  data.get('database', ''), data.get('username', ''), data.get('password', ''),
                  data.get('file_path', ''), json.dumps(data.get('options', {})), now, now))
            data['id'] = c.lastrowid
        self.conn.commit()
        return data['id']

    def delete_config(self, config_id: int) -> bool:
        c = self.conn.cursor()
        c.execute("DELETE FROM configs WHERE id=?", (config_id,))
        c.execute("DELETE FROM extractions WHERE config_id=?", (config_id,))
        self.conn.commit()
        return c.rowcount > 0

    def save_extraction(self, config_id: int, config_name: str, status: str,
                        tables_found: int = 0, schema_json: str = None,
                        schema_sql: str = None, error_message: str = None) -> int:
        now = datetime.now().isoformat()
        c = self.conn.cursor()
        c.execute("""
            INSERT INTO extractions (config_id, config_name, status, tables_found, schema_json, schema_sql, error_message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (config_id, config_name, status, tables_found, schema_json, schema_sql, error_message, now))
        self.conn.commit()
        return c.lastrowid

    def get_extractions(self, config_id: int) -> List[Dict]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM extractions WHERE config_id=? ORDER BY created_at DESC", (config_id,))
        return [dict(row) for row in c.fetchall()]

    def get_extraction(self, extraction_id: int) -> Optional[Dict]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM extractions WHERE id=?", (extraction_id,))
        row = c.fetchone()
        return dict(row) if row else None

    def get_all_extractions(self) -> List[Dict]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM extractions ORDER BY created_at DESC LIMIT 50")
        return [dict(row) for row in c.fetchall()]

    def close(self):
        self.conn.close()


# --- COMPONENTS ---

class ToastNotification:
    @staticmethod
    def show(master, message, type="success", duration=3000):
        color = C_SUCCESS if type == "success" else C_DANGER if type == "error" else (THEME["light"]["warning"], THEME["dark"]["warning"])
        toast = ctk.CTkFrame(master, fg_color=color, corner_radius=8)
        lbl = ctk.CTkLabel(toast, text=message, text_color=("#FFFFFF", "#FFFFFF"), font=("Segoe UI", 13, "bold"))
        lbl.pack(padx=20, pady=12)
        toast.place(relx=0.98, rely=0.95, anchor="se")
        def destroy():
            try: toast.destroy()
            except: pass
        master.after(duration, destroy)

class StatCard(ctk.CTkFrame):
    def __init__(self, master, title, value):
        super().__init__(master, fg_color=C_SURFACE, corner_radius=8, border_width=1, border_color=C_BORDER)
        self.pack_propagate(False)
        self.configure(width=160, height=80)
        ctk.CTkLabel(self, text=title, font=("Segoe UI", 12), text_color=C_TEXT_SECONDARY).pack(anchor="w", padx=16, pady=(12, 0))
        self.val_label = ctk.CTkLabel(self, text=str(value), font=("Segoe UI", 24, "bold"), text_color=C_TEXT_PRIMARY)
        self.val_label.pack(anchor="w", padx=16, pady=(0, 12))
        
    def update_value(self, new_val):
        self.val_label.configure(text=str(new_val))

class ConfigDialog(ctk.CTkToplevel):
    def __init__(self, parent, config=None):
        super().__init__(parent)
        self.result = None
        self.config = config
        self.title("Edit Configuration" if config else "New Configuration")
        self.geometry("520x620")
        self.resizable(False, False)
        self.transient(parent)
        self.configure(fg_color=C_BG)
        self.grab_set()
        self._build_form()
        if config:
            self._populate(config)

    def _build_form(self):
        main = ctk.CTkFrame(self, fg_color=C_SURFACE, corner_radius=12, border_width=1, border_color=C_BORDER)
        main.pack(fill="both", expand=True, padx=24, pady=24)

        ctk.CTkLabel(main, text="Configuration Name", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY).pack(anchor="w", padx=20, pady=(20, 4))
        self.name_entry = ctk.CTkEntry(main, placeholder_text="e.g. production-db", height=38, border_color=C_BORDER, fg_color=C_BG)
        self.name_entry.pack(fill="x", padx=20, pady=(0, 12))

        ctk.CTkLabel(main, text="Database Type", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY).pack(anchor="w", padx=20, pady=(0, 4))
        self.type_var = ctk.StringVar(value="mysql")
        self.type_menu = ctk.CTkOptionMenu(main, variable=self.type_var, values=["mysql", "postgres", "sqlite", "sqlserver"], 
                                           height=38, fg_color=C_SURFACE, button_color=C_BORDER, button_hover_color=C_BORDER, 
                                           text_color=C_TEXT_PRIMARY, command=self._on_type_change)
        self.type_menu.pack(fill="x", padx=20, pady=(0, 12))

        self.remote_frame = ctk.CTkFrame(main, fg_color="transparent")
        self.remote_frame.pack(fill="x", padx=20)

        ctk.CTkLabel(self.remote_frame, text="Host", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY).pack(anchor="w", pady=(0, 4))
        self.host_entry = ctk.CTkEntry(self.remote_frame, placeholder_text="localhost", height=38, border_color=C_BORDER, fg_color=C_BG)
        self.host_entry.pack(fill="x", pady=(0, 12))

        ctk.CTkLabel(self.remote_frame, text="Port", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY).pack(anchor="w", pady=(0, 4))
        self.port_entry = ctk.CTkEntry(self.remote_frame, placeholder_text="3306", height=38, border_color=C_BORDER, fg_color=C_BG)
        self.port_entry.pack(fill="x", pady=(0, 12))

        ctk.CTkLabel(self.remote_frame, text="Database", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY).pack(anchor="w", pady=(0, 4))
        self.db_entry = ctk.CTkEntry(self.remote_frame, placeholder_text="database name", height=38, border_color=C_BORDER, fg_color=C_BG)
        self.db_entry.pack(fill="x", pady=(0, 12))

        ctk.CTkLabel(self.remote_frame, text="Username", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY).pack(anchor="w", pady=(0, 4))
        self.user_entry = ctk.CTkEntry(self.remote_frame, placeholder_text="root", height=38, border_color=C_BORDER, fg_color=C_BG)
        self.user_entry.pack(fill="x", pady=(0, 12))

        ctk.CTkLabel(self.remote_frame, text="Password", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY).pack(anchor="w", pady=(0, 4))
        self.pass_entry = ctk.CTkEntry(self.remote_frame, placeholder_text="password", show="*", height=38, border_color=C_BORDER, fg_color=C_BG)
        self.pass_entry.pack(fill="x", pady=(0, 12))

        self.sqlite_frame = ctk.CTkFrame(main, fg_color="transparent")

        ctk.CTkLabel(self.sqlite_frame, text="Database File Path", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY).pack(anchor="w", pady=(0, 4))
        path_row = ctk.CTkFrame(self.sqlite_frame, fg_color="transparent")
        path_row.pack(fill="x", pady=(0, 12))
        self.file_entry = ctk.CTkEntry(path_row, placeholder_text="/path/to/database.db", height=38, border_color=C_BORDER, fg_color=C_BG)
        self.file_entry.pack(side="left", fill="x", expand=True, padx=(0, 8))
        ctk.CTkButton(path_row, text="Browse", width=80, height=38, fg_color=C_BORDER, hover_color=C_SIDEBAR, text_color=C_TEXT_PRIMARY, command=self._browse).pack(side="right")

        btn_row = ctk.CTkFrame(main, fg_color="transparent")
        btn_row.pack(fill="x", padx=20, pady=(20, 20))
        ctk.CTkButton(btn_row, text="Cancel", width=100, height=38, fg_color=C_BORDER, hover_color=C_SIDEBAR, text_color=C_TEXT_PRIMARY, command=self.destroy).pack(side="right", padx=(8, 0))
        ctk.CTkButton(btn_row, text="Save Config", width=120, height=38, fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER, command=self._save).pack(side="right")

    def _on_type_change(self, choice):
        if choice == "sqlite":
            self.remote_frame.pack_forget()
            self.sqlite_frame.pack(fill="x", padx=20)
        else:
            self.sqlite_frame.pack_forget()
            self.remote_frame.pack(fill="x", padx=20)

    def _browse(self):
        path = filedialog.askopenfilename(filetypes=[("SQLite files", "*.db *.sqlite3 *.sqlite"), ("All files", "*.*")])
        if path:
            self.file_entry.delete(0, "end")
            self.file_entry.insert(0, path)

    def _populate(self, config):
        self.name_entry.insert(0, config.get('name', ''))
        self.type_var.set(config.get('db_type', 'mysql'))
        self._on_type_change(config.get('db_type', 'mysql'))
        self.host_entry.insert(0, config.get('host', ''))
        self.port_entry.insert(0, str(config.get('port', '')))
        self.db_entry.insert(0, config.get('database', ''))
        self.user_entry.insert(0, config.get('username', ''))
        self.pass_entry.insert(0, config.get('password', ''))
        self.file_entry.insert(0, config.get('file_path', ''))

    def _save(self):
        name = self.name_entry.get().strip()
        if not name:
            messagebox.showerror("Error", "Configuration name is required", parent=self)
            return

        self.result = {
            'name': name,
            'db_type': self.type_var.get(),
            'host': self.host_entry.get().strip(),
            'port': int(self.port_entry.get().strip() or 0),
            'database': self.db_entry.get().strip(),
            'username': self.user_entry.get().strip(),
            'password': self.pass_entry.get().strip(),
            'file_path': self.file_entry.get().strip(),
        }
        if self.config and self.config.get('id'):
            self.result['id'] = self.config['id']
        self.destroy()

# --- MAIN APP ---

class MainApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("DB Extractor")
        self.geometry("1400x850")
        self.minsize(1100, 700)
        
        self.db = AppDB()
        self.current_config_id = None
        self.all_configs = []
        
        self.configure(fg_color=C_BG)
        ctk.set_appearance_mode("System")
        
        self._build_ui()
        self._update_treeview_style()
        self._load_data()
        
    def _build_ui(self):
        # Base Layout
        self.sidebar = ctk.CTkFrame(self, width=240, fg_color=C_SIDEBAR, corner_radius=0)
        self.sidebar.pack(side="left", fill="y")
        self.sidebar.pack_propagate(False)

        self.main_area = ctk.CTkFrame(self, fg_color=C_BG, corner_radius=0)
        self.main_area.pack(side="left", fill="both", expand=True)

        self._build_sidebar()
        self._build_main_area()
        self._build_context_menu()

    def _build_sidebar(self):
        # Header
        ctk.CTkLabel(self.sidebar, text="DB Extractor", font=("Segoe UI", 20, "bold"), text_color=C_PRIMARY).pack(pady=(24, 4), padx=24, anchor="w")
        ctk.CTkLabel(self.sidebar, text="Schema Manager", font=("Segoe UI", 12), text_color=C_TEXT_SECONDARY).pack(padx=24, anchor="w", pady=(0, 32))

        # Navigation
        self.nav_configs_btn = ctk.CTkButton(self.sidebar, text="  Configurations", anchor="w", font=("Segoe UI", 14), height=40, fg_color=C_BORDER, hover_color=C_BORDER, text_color=C_TEXT_PRIMARY, corner_radius=6, command=lambda: self._switch_nav('configs'))
        self.nav_configs_btn.pack(fill="x", padx=16, pady=4)
        
        self.nav_history_btn = ctk.CTkButton(self.sidebar, text="  Extraction History", anchor="w", font=("Segoe UI", 14), height=40, fg_color="transparent", hover_color=C_BORDER, text_color=C_TEXT_SECONDARY, corner_radius=6, command=lambda: self._switch_nav('history'))
        self.nav_history_btn.pack(fill="x", padx=16, pady=4)

        ctk.CTkFrame(self.sidebar, height=1, fg_color=C_BORDER).pack(fill="x", padx=24, pady=24)

        # Supported DBs
        ctk.CTkLabel(self.sidebar, text="SUPPORTED DATABASES", font=("Segoe UI", 11, "bold"), text_color=C_TEXT_SECONDARY).pack(padx=24, anchor="w", pady=(0, 12))
        
        db_frame = ctk.CTkFrame(self.sidebar, fg_color="transparent")
        db_frame.pack(fill="x", padx=24)
        
        for db in ["MySQL", "PostgreSQL", "SQLite", "SQL Server"]:
            bg, fg = get_badge_color(db)
            ctk.CTkLabel(db_frame, text=f" {db} ", font=("Segoe UI", 11, "bold"), fg_color=bg, text_color=fg, corner_radius=4).pack(anchor="w", pady=4)

        # Theme Selector at bottom
        bottom_frame = ctk.CTkFrame(self.sidebar, fg_color="transparent")
        bottom_frame.pack(side="bottom", fill="x", padx=24, pady=24)
        ctk.CTkLabel(bottom_frame, text="Theme", font=("Segoe UI", 12, "bold"), text_color=C_TEXT_SECONDARY).pack(anchor="w", pady=(0, 4))
        self.theme_menu = ctk.CTkOptionMenu(bottom_frame, values=["System", "Light", "Dark"], fg_color=C_SURFACE, button_color=C_BORDER, button_hover_color=C_BORDER, text_color=C_TEXT_PRIMARY, command=self._change_theme)
        self.theme_menu.pack(fill="x")

    def _build_main_area(self):
        self.content_padding = ctk.CTkFrame(self.main_area, fg_color="transparent")
        self.content_padding.pack(fill="both", expand=True, padx=32, pady=32)

        # Header
        self.header_frame = ctk.CTkFrame(self.content_padding, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 24))
        
        title_box = ctk.CTkFrame(self.header_frame, fg_color="transparent")
        title_box.pack(side="left")
        self.page_title = ctk.CTkLabel(title_box, text="Configurations", font=("Segoe UI", 28, "bold"), text_color=C_TEXT_PRIMARY)
        self.page_title.pack(anchor="w")
        self.page_subtitle = ctk.CTkLabel(title_box, text="Manage database connections and extract schemas", font=("Segoe UI", 14), text_color=C_TEXT_SECONDARY)
        self.page_subtitle.pack(anchor="w")

        actions_box = ctk.CTkFrame(self.header_frame, fg_color="transparent")
        actions_box.pack(side="right", anchor="s")
        
        self.configs_actions_frame = ctk.CTkFrame(actions_box, fg_color="transparent")
        self.search_var = ctk.StringVar()
        self.search_var.trace_add("write", lambda *_: self._filter_configs())
        self.search_entry = ctk.CTkEntry(self.configs_actions_frame, textvariable=self.search_var, placeholder_text="Search configs...", width=240, height=40, border_color=C_BORDER, fg_color=C_SURFACE)
        self.search_entry.pack(side="left", padx=(0, 16))
        
        self.new_btn = ctk.CTkButton(self.configs_actions_frame, text="+ New Config", fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER, height=40, font=("Segoe UI", 14, "bold"), command=self._add_config)
        self.new_btn.pack(side="left")
        self.configs_actions_frame.pack(side="left")

        self.hist_actions_frame = ctk.CTkFrame(actions_box, fg_color="transparent")
        self.hist_search_var = ctk.StringVar()
        self.hist_search_var.trace_add("write", lambda *_: self._filter_history())
        self.hist_search_entry = ctk.CTkEntry(self.hist_actions_frame, textvariable=self.hist_search_var, placeholder_text="Search history...", width=240, height=40, border_color=C_BORDER, fg_color=C_SURFACE)
        self.hist_search_entry.pack(side="left", padx=(0, 16))
        self.hist_status_var = ctk.StringVar(value="All")
        self.hist_status_menu = ctk.CTkOptionMenu(self.hist_actions_frame, values=["All", "Success", "Failed"], variable=self.hist_status_var, command=lambda *_: self._filter_history(), fg_color=C_SURFACE, button_color=C_BORDER, button_hover_color=C_BORDER, text_color=C_TEXT_PRIMARY, height=40, width=120)
        self.hist_status_menu.pack(side="left")

        # Stats
        self.stats_frame = ctk.CTkFrame(self.content_padding, fg_color="transparent")
        self.stats_frame.pack(fill="x", pady=(0, 24))
        self.stat_total = StatCard(self.stats_frame, "Total Configs", "0")
        self.stat_total.pack(side="left", padx=(0, 16))
        self.stat_mysql = StatCard(self.stats_frame, "MySQL", "0")
        self.stat_mysql.pack(side="left", padx=(0, 16))
        self.stat_sqlite = StatCard(self.stats_frame, "SQLite", "0")
        self.stat_sqlite.pack(side="left", padx=(0, 16))
        self.stat_last = StatCard(self.stats_frame, "Last Extraction", "-")
        self.stat_last.pack(side="left")

        # Split View for Table and Details
        self.split_view = ctk.CTkFrame(self.content_padding, fg_color="transparent")
        self.split_view.pack(fill="both", expand=True)

        self.table_card = ctk.CTkFrame(self.split_view, fg_color=C_SURFACE, corner_radius=12, border_width=1, border_color=C_BORDER)
        self.table_card.pack(side="left", fill="both", expand=True)
        self.table_card.pack_propagate(False)

        self._build_table()
        self._build_details_panel()

    def _build_table(self):
        self.config_tree = ttk.Treeview(self.table_card, columns=("name", "type", "host", "database", "updated", "status"), show="headings", selectmode="browse")
        self.config_tree.heading("name", text="Name")
        self.config_tree.heading("type", text="Type")
        self.config_tree.heading("host", text="Host / Path")
        self.config_tree.heading("database", text="Database")
        self.config_tree.heading("updated", text="Last Updated")
        self.config_tree.heading("status", text="Status")
        
        self.config_tree.column("name", width=160, minwidth=120)
        self.config_tree.column("type", width=100, minwidth=80)
        self.config_tree.column("host", width=200, minwidth=100)
        self.config_tree.column("database", width=140, minwidth=100)
        self.config_tree.column("updated", width=140, minwidth=120)
        self.config_tree.column("status", width=120, minwidth=100)
        
        self.config_tree.pack(fill="both", expand=True, padx=2, pady=2)
        
        self.config_tree.bind("<<TreeviewSelect>>", self._on_select_config)
        self.config_tree.bind("<Button-3>", self._show_context_menu)
        self.config_tree.bind("<Double-1>", self._on_global_hist_double_click)
        
    def _build_details_panel(self):
        self.details_panel = ctk.CTkFrame(self.split_view, width=400, fg_color=C_SURFACE, corner_radius=12, border_width=1, border_color=C_BORDER)
        self.details_panel.pack_propagate(False)
        # Hidden by default

        self.dp_header = ctk.CTkFrame(self.details_panel, fg_color="transparent")
        self.dp_header.pack(fill="x", padx=24, pady=(24, 16))
        
        self.dp_title = ctk.CTkLabel(self.dp_header, text="Config Name", font=("Segoe UI", 20, "bold"), text_color=C_TEXT_PRIMARY)
        self.dp_title.pack(anchor="w")
        self.dp_type_badge = ctk.CTkLabel(self.dp_header, text="TYPE", font=("Segoe UI", 11, "bold"), corner_radius=4, height=22)
        self.dp_type_badge.pack(anchor="w", pady=(8, 0))

        actions_frame = ctk.CTkFrame(self.details_panel, fg_color="transparent")
        actions_frame.pack(fill="x", padx=24, pady=(0, 20))
        
        self.btn_extract = ctk.CTkButton(actions_frame, text="Extract Schema", fg_color=C_SUCCESS, hover_color=C_SUCCESS_HOVER, height=36, font=("Segoe UI", 13, "bold"), command=self._extract_selected)
        self.btn_extract.pack(side="left", padx=(0, 8), fill="x", expand=True)
        self.btn_edit = ctk.CTkButton(actions_frame, text="Edit", fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER, height=36, font=("Segoe UI", 13, "bold"), command=self._edit_selected)
        self.btn_edit.pack(side="left", padx=(0, 8), fill="x", expand=True)
        self.btn_delete = ctk.CTkButton(actions_frame, text="Delete", fg_color=C_DANGER, hover_color=C_DANGER_HOVER, height=36, font=("Segoe UI", 13, "bold"), command=self._delete_selected)
        self.btn_delete.pack(side="left", fill="x", expand=True)

        self.details_tabs = ctk.CTkTabview(self.details_panel, fg_color="transparent", segmented_button_fg_color=C_BG, segmented_button_selected_color=C_PRIMARY, segmented_button_selected_hover_color=C_PRIMARY_HOVER)
        self.details_tabs.pack(fill="both", expand=True, padx=24, pady=(0, 24))
        
        self.details_tabs.add("Overview")
        self.details_tabs.add("Schema")
        self.details_tabs.add("History")
        
        self._build_dp_overview(self.details_tabs.tab("Overview"))
        self._build_dp_schema(self.details_tabs.tab("Schema"))
        self._build_dp_history(self.details_tabs.tab("History"))
        
    def _build_dp_overview(self, parent):
        self.ov_frame = ctk.CTkFrame(parent, fg_color="transparent")
        self.ov_frame.pack(fill="both", expand=True, pady=12)
        self.ov_labels = {}
        fields = ["Host / Path", "Database", "Username", "Created", "Last Updated"]
        for f in fields:
            row = ctk.CTkFrame(self.ov_frame, fg_color="transparent")
            row.pack(fill="x", pady=6)
            ctk.CTkLabel(row, text=f, font=("Segoe UI", 12), text_color=C_TEXT_SECONDARY, width=100, anchor="w").pack(side="left")
            val = ctk.CTkLabel(row, text="-", font=("Segoe UI", 13, "bold"), text_color=C_TEXT_PRIMARY, anchor="w")
            val.pack(side="left", fill="x", expand=True, padx=8)
            self.ov_labels[f] = val

    def _build_dp_schema(self, parent):
        self.schema_actions = ctk.CTkFrame(parent, fg_color="transparent")
        self.schema_actions.pack(fill="x", pady=(12, 0))
        self.btn_view_json = ctk.CTkButton(self.schema_actions, text="View JSON", fg_color=C_BORDER, hover_color=C_SIDEBAR, text_color=C_TEXT_PRIMARY, height=28, command=lambda: None)
        self.btn_view_json.pack(side="left", padx=(0, 8), fill="x", expand=True)
        self.btn_view_sql = ctk.CTkButton(self.schema_actions, text="View SQL DDL", fg_color=C_BORDER, hover_color=C_SIDEBAR, text_color=C_TEXT_PRIMARY, height=28, command=lambda: None)
        self.btn_view_sql.pack(side="left", fill="x", expand=True)

        self.schema_frame = ctk.CTkFrame(parent, fg_color="transparent")
        self.schema_frame.pack(fill="both", expand=True, pady=12)
        
        self.schema_tree = ttk.Treeview(self.schema_frame, columns=("table", "cols", "keys"), show="headings", selectmode="browse")
        self.schema_tree.heading("table", text="Table")
        self.schema_tree.heading("cols", text="Cols")
        self.schema_tree.heading("keys", text="Keys")
        self.schema_tree.column("table", width=150)
        self.schema_tree.column("cols", width=50)
        self.schema_tree.column("keys", width=50)
        self.schema_tree.pack(fill="both", expand=True)
        self.schema_tree.bind("<Double-1>", lambda e: self._view_table_detail(self.current_schema, self.schema_tree))

    def _build_dp_history(self, parent):
        self.hist_frame = ctk.CTkFrame(parent, fg_color="transparent")
        self.hist_frame.pack(fill="both", expand=True, pady=12)
        
        self.hist_tree = ttk.Treeview(self.hist_frame, columns=("date", "status"), show="headings", selectmode="browse")
        self.hist_tree.heading("date", text="Date")
        self.hist_tree.heading("status", text="Status")
        self.hist_tree.column("date", width=150)
        self.hist_tree.column("status", width=100)
        self.hist_tree.pack(fill="both", expand=True)
        self.hist_tree.bind("<Double-1>", self._on_hist_double_click)

    def _build_context_menu(self):
        self.context_menu = tk.Menu(self, tearoff=0, bg=THEME["light"]["surface"], fg=THEME["light"]["text_primary"], borderwidth=0, activebackground=THEME["light"]["primary"], activeforeground="#ffffff")
        self.context_menu.add_command(label="Open Details", command=lambda: None) # It opens on click anyway
        self.context_menu.add_command(label="Extract Schema", command=self._extract_selected)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Edit", command=self._edit_selected)
        self.context_menu.add_command(label="Delete", command=self._delete_selected)

    def _show_context_menu(self, event):
        if self.page_title.cget("text") != "Configurations":
            return
        item = self.config_tree.identify_row(event.y)
        if item:
            self.config_tree.selection_set(item)
            self._on_select_config(None)
            self.context_menu.tk_popup(event.x_root, event.y_root)

    def _switch_nav(self, tab):
        if tab == 'configs':
            self.nav_configs_btn.configure(fg_color=C_BORDER, text_color=C_TEXT_PRIMARY)
            self.nav_history_btn.configure(fg_color="transparent", text_color=C_TEXT_SECONDARY)
            self.stats_frame.pack(fill="x", pady=(0, 24), after=self.header_frame)
            self.page_title.configure(text="Configurations")
            self.page_subtitle.configure(text="Manage database connections and extract schemas")
            self.hist_actions_frame.pack_forget()
            self.configs_actions_frame.pack(side="left")
            self._load_data()
        else:
            self.nav_configs_btn.configure(fg_color="transparent", text_color=C_TEXT_SECONDARY)
            self.nav_history_btn.configure(fg_color=C_BORDER, text_color=C_TEXT_PRIMARY)
            self.stats_frame.pack_forget()
            self.details_panel.pack_forget()
            self.page_title.configure(text="Extraction History")
            self.page_subtitle.configure(text="View all past schema extractions across databases")
            self.configs_actions_frame.pack_forget()
            self.hist_actions_frame.pack(side="left")
            self._load_history_view()

    def _change_theme(self, new_theme: str):
        ctk.set_appearance_mode(new_theme)
        self._update_treeview_style()

    def _update_treeview_style(self):
        style = ttk.Style()
        style.theme_use("clam")
        mode = ctk.get_appearance_mode()
        
        bg = THEME["dark"]["tree_bg"] if mode == "Dark" else THEME["light"]["tree_bg"]
        fg = THEME["dark"]["tree_fg"] if mode == "Dark" else THEME["light"]["tree_fg"]
        sel_bg = THEME["dark"]["tree_sel_bg"] if mode == "Dark" else THEME["light"]["tree_sel_bg"]
        sel_fg = THEME["dark"]["tree_sel_fg"] if mode == "Dark" else THEME["light"]["tree_sel_fg"]
        head_bg = THEME["dark"]["sidebar"] if mode == "Dark" else THEME["light"]["sidebar"]
        
        style.configure("Treeview", background=bg, foreground=fg, fieldbackground=bg, rowheight=44, borderwidth=0, font=("Segoe UI", 12))
        style.configure("Treeview.Heading", background=head_bg, foreground=fg, font=("Segoe UI", 12, "bold"), relief="flat", borderwidth=0, padding=8)
        style.map("Treeview", background=[("selected", sel_bg)], foreground=[("selected", sel_fg)])

    def _load_data(self):
        self.all_configs = self.db.get_all_configs()
        self._update_stats()
        self._filter_configs()
        
    def _update_stats(self):
        self.stat_total.update_value(len(self.all_configs))
        self.stat_mysql.update_value(sum(1 for c in self.all_configs if c['db_type'] == 'mysql'))
        self.stat_sqlite.update_value(sum(1 for c in self.all_configs if c['db_type'] == 'sqlite'))
        
        extractions = self.db.get_all_extractions()
        if extractions:
            latest = extractions[0].get('created_at', '')[:10]
            self.stat_last.update_value(latest)
        else:
            self.stat_last.update_value("Never")

    def _filter_configs(self):
        q = self.search_var.get().strip().lower()
        for item in self.config_tree.get_children():
            self.config_tree.delete(item)
            
        for cfg in self.all_configs:
            if q and not (q in cfg['name'].lower() or q in cfg['db_type'].lower() or q in (cfg.get('database') or '').lower()):
                continue
            
            host_display = cfg.get('file_path') or f"{cfg.get('host')}:{cfg.get('port')}"
            if len(host_display) > 30: host_display = host_display[:27] + "..."
            
            dt = cfg.get('updated_at', '')[:16].replace('T', ' ')
            
            # Find status
            exs = self.db.get_extractions(cfg['id'])
            status = exs[0]['status'].capitalize() if exs else "Not Extracted"
            
            self.config_tree.insert("", "end", iid=str(cfg['id']), values=(
                cfg['name'], cfg['db_type'].upper(), host_display, cfg.get('database', '-'), dt, status
            ))

    def _on_select_config(self, event):
        sel = self.config_tree.selection()
        if not sel or self.page_title.cget("text") != "Configurations":
            self.details_panel.pack_forget()
            return
            
        self.current_config_id = int(sel[0])
        cfg = self.db.get_config(self.current_config_id)
        if not cfg: return
        
        # Show panel
        self.details_panel.pack(side="right", fill="y", padx=(16, 0))
        
        self.dp_title.configure(text=cfg['name'])
        bg, fg = get_badge_color(cfg['db_type'])
        self.dp_type_badge.configure(text=f" {cfg['db_type'].upper()} ", fg_color=bg, text_color=fg)
        
        host_display = cfg.get('file_path') or f"{cfg.get('host')}:{cfg.get('port')}"
        self.ov_labels["Host / Path"].configure(text=host_display)
        self.ov_labels["Database"].configure(text=cfg.get('database') or '-')
        self.ov_labels["Username"].configure(text=cfg.get('username') or '-')
        self.ov_labels["Created"].configure(text=cfg.get('created_at', '')[:16].replace('T', ' '))
        self.ov_labels["Last Updated"].configure(text=cfg.get('updated_at', '')[:16].replace('T', ' '))
        
        # Load Schema
        for item in self.schema_tree.get_children(): self.schema_tree.delete(item)
        extractions = self.db.get_extractions(cfg['id'])
        success = [e for e in extractions if e.get('status') == 'success']
        
        self.current_schema = None
        self.btn_view_json.configure(state="disabled", command=lambda: None)
        self.btn_view_sql.configure(state="disabled", command=lambda: None)
        
        if success:
            schema_path = success[0].get('schema_json')
            sql_path = success[0].get('schema_sql')
            
            if schema_path and os.path.exists(schema_path):
                self.btn_view_json.configure(state="normal", command=lambda: self._view_file(schema_path))
                try:
                    with open(schema_path, 'r') as f:
                        self.current_schema = json.load(f)
                        for t in self.current_schema.get('tables', []):
                            self.schema_tree.insert("", "end", values=(
                                t['name'], len(t.get('columns', [])), len(t.get('foreign_keys', []))
                            ))
                except: pass
            
            if sql_path and os.path.exists(sql_path):
                self.btn_view_sql.configure(state="normal", command=lambda: self._view_file(sql_path))
                
        # Load History
        for item in self.hist_tree.get_children(): self.hist_tree.delete(item)
        self.current_extractions = {}
        for ex in extractions:
            eid = str(ex['id'])
            self.current_extractions[eid] = ex
            self.hist_tree.insert("", "end", iid=eid, values=(ex.get('created_at', '')[:16].replace('T', ' '), ex.get('status', '').capitalize()))

    def _add_config(self):
        dialog = ConfigDialog(self)
        self.wait_window(dialog)
        if dialog.result:
            self.db.save_config(dialog.result)
            self._load_data()
            ToastNotification.show(self, "Configuration saved successfully!")

    def _edit_selected(self):
        if not self.current_config_id: return
        cfg = self.db.get_config(self.current_config_id)
        if not cfg: return
        dialog = ConfigDialog(self, config=cfg)
        self.wait_window(dialog)
        if dialog.result:
            self.db.save_config(dialog.result)
            self._load_data()
            self._on_select_config(None) # Refresh details
            ToastNotification.show(self, "Configuration updated!")

    def _delete_selected(self):
        if not self.current_config_id: return
        if messagebox.askyesno("Confirm Delete", "Are you sure you want to delete this configuration? This cannot be undone.", parent=self):
            self.db.delete_config(self.current_config_id)
            self.details_panel.pack_forget()
            self._load_data()
            ToastNotification.show(self, "Configuration deleted.", "error")

    def _extract_selected(self):
        if not self.current_config_id: return
        cfg = self.db.get_config(self.current_config_id)
        
        self.btn_extract.configure(text="Extracting...", state="disabled")
        
        def run():
            try:
                db_config = DBConfig(
                    name=cfg['name'], db_type=cfg['db_type'], host=cfg.get('host', ''),
                    port=cfg.get('port', 0), database=cfg.get('database', ''),
                    username=cfg.get('username', ''), password=cfg.get('password', ''),
                    file_path=cfg.get('file_path', '')
                )
                extractor = SchemaExtractor(db_config)
                schema = extractor.extract_schema()
                
                if not schema or not schema.get("tables"):
                    self.after(0, lambda: self._on_extract_finish(cfg, 'failed', "No tables found"))
                    return

                json_path = str(SCHEMA_OUTPUT_DIR / f"{cfg['name']}_schema.json")
                extractor.save_schema_to_file(schema, json_path)
                ddl = extractor.generate_sql_ddl(schema)
                sql_path = str(SCHEMA_OUTPUT_DIR / f"{cfg['name']}_schema.sql")
                with open(sql_path, 'w') as f: f.write(ddl)
                extractor.disconnect()

                self.after(0, lambda: self._on_extract_finish(cfg, 'success', None, len(schema['tables']), json_path, sql_path))
            except Exception as e:
                self.after(0, lambda: self._on_extract_finish(cfg, 'failed', str(e)))

        threading.Thread(target=run, daemon=True).start()

    def _on_extract_finish(self, cfg, status, error=None, tables=0, j_path=None, s_path=None):
        self.db.save_extraction(cfg['id'], cfg['name'], status, tables, j_path, s_path, error)
        self.btn_extract.configure(text="Extract Schema", state="normal")
        self._load_data()
        self._on_select_config(None)
        if status == 'success':
            ToastNotification.show(self, f"Extracted {tables} tables successfully!")
        else:
            messagebox.showerror("Extraction Failed", error)

    def _load_history_view(self):
        for item in self.config_tree.get_children(): self.config_tree.delete(item)
        self.config_tree.heading("name", text="Date")
        self.config_tree.heading("type", text="Config Name")
        self.config_tree.heading("host", text="Status")
        self.config_tree.heading("database", text="Tables")
        self.config_tree.heading("updated", text="Details")
        self.config_tree.heading("status", text="")
        
        all_ex = self.db.get_all_extractions()
        self.global_extractions = {}
        for ex in all_ex:
            dt = ex.get('created_at', '')[:19].replace('T', ' ')
            detail = ex.get('error_message', '') or ex.get('schema_json', '') or ''
            if len(detail) > 60: detail = detail[:57] + "..."
            eid = str(ex['id'])
            self.global_extractions[eid] = ex
            
        self._filter_history()

    def _filter_history(self):
        if self.page_title.cget("text") != "Extraction History": return
        for item in self.config_tree.get_children(): self.config_tree.delete(item)
        
        q = self.hist_search_var.get().strip().lower()
        status_f = self.hist_status_var.get().lower()
        
        for eid, ex in self.global_extractions.items():
            ex_status = ex.get('status', '').lower()
            if status_f != "all" and ex_status != status_f:
                continue
            
            cfg_name = ex.get('config_name', '').lower()
            err_msg = ex.get('error_message', '') or ex.get('schema_json', '') or ''
            
            if q and not (q in cfg_name or q in err_msg.lower()):
                continue
                
            dt = ex.get('created_at', '')[:19].replace('T', ' ')
            detail = err_msg
            if len(detail) > 60: detail = detail[:57] + "..."
            
            self.config_tree.insert("", "end", iid=eid, values=(dt, ex.get('config_name', ''), ex_status.capitalize(), ex.get('tables_found', 0), detail, ''))
            
    def _on_hist_double_click(self, event):
        sel = self.hist_tree.selection()
        if not sel: return
        ex = self.current_extractions.get(sel[0])
        if not ex: return
        path = ex.get('schema_sql') or ex.get('schema_json')
        if path: self._view_file(path)

    def _on_global_hist_double_click(self, event):
        if self.page_title.cget("text") != "Extraction History": return
        sel = self.config_tree.selection()
        if not sel: return
        ex = self.global_extractions.get(sel[0])
        if not ex: return
        path = ex.get('schema_sql') or ex.get('schema_json')
        if path: self._view_file(path)

    def _view_table_detail(self, schema, tree):
        if not schema: return
        sel = tree.selection()
        if not sel: return
        table_name = tree.item(sel[0])['values'][0]
        table = next((t for t in schema.get('tables', []) if t['name'] == table_name), None)
        if not table: return

        detail = ctk.CTkToplevel(self)
        detail.title(f"Table: {table_name}")
        detail.geometry("700x500")
        detail.transient(self)
        detail.configure(fg_color=C_BG)

        text = ctk.CTkTextbox(detail, font=("Consolas", 13), fg_color=C_SURFACE, text_color=C_TEXT_PRIMARY, border_color=C_BORDER, border_width=1)
        text.pack(fill="both", expand=True, padx=12, pady=12)

        lines = [f"TABLE: {table_name}", "=" * 60, "", "COLUMNS:", "-" * 40]
        for col in table.get('columns', []):
            pk = " [PK]" if col.get('primary_key') else ""
            nn = " NOT NULL" if not col.get('nullable', True) else ""
            default = f" DEFAULT {col['default']}" if col.get('default') is not None else ""
            lines.append(f"  {col['name']}: {col['type']}{pk}{nn}{default}")

        fks = table.get('foreign_keys', [])
        if fks:
            lines.extend(["", "FOREIGN KEYS:", "-" * 40])
            for fk in fks:
                lines.append(f"  {fk['column']} -> {fk['references_table']}.{fk['references_column']}")

        indexes = table.get('indexes', [])
        if indexes:
            lines.extend(["", "INDEXES:", "-" * 40])
            for idx in indexes:
                unique = " [UNIQUE]" if idx.get('unique') else ""
                lines.append(f"  {idx['name']}: {', '.join(idx.get('columns', []))}{unique}")

        text.insert("1.0", "\n".join(lines))
        text.configure(state="disabled")

    def _view_file(self, path):
        if not path or not os.path.exists(path):
            messagebox.showwarning("Warning", "File not found", parent=self)
            return
        viewer = ctk.CTkToplevel(self)
        viewer.title(os.path.basename(path))
        viewer.geometry("900x650")
        viewer.transient(self)
        viewer.configure(fg_color=C_BG)

        header = ctk.CTkFrame(viewer, fg_color=C_SURFACE, height=48, corner_radius=0, border_width=1, border_color=C_BORDER)
        header.pack(fill="x")
        ctk.CTkLabel(header, text=os.path.basename(path), font=("Segoe UI", 14, "bold"), text_color=C_TEXT_PRIMARY).pack(side="left", padx=16)

        def copy_to_clipboard():
            viewer.clipboard_clear()
            viewer.clipboard_append(content)
            copy_btn.configure(text="Copied!")
            viewer.after(1500, lambda: copy_btn.configure(text="Copy to Clipboard"))

        copy_btn = ctk.CTkButton(header, text="Copy to Clipboard", width=120, height=32, fg_color=C_PRIMARY, hover_color=C_PRIMARY_HOVER, text_color="#FFFFFF", font=("Segoe UI", 12, "bold"), command=copy_to_clipboard)
        copy_btn.pack(side="right", padx=16)

        text = ctk.CTkTextbox(viewer, font=("Consolas", 13), fg_color=C_SURFACE, text_color=C_TEXT_PRIMARY, border_color=C_BORDER, border_width=1)
        text.pack(fill="both", expand=True, padx=16, pady=16)
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            text.insert("1.0", content)
        except Exception as e:
            text.insert("1.0", f"Error reading file: {e}")
            content = ""
        text.configure(state="disabled")
            
    def on_closing(self):
        self.db.close()
        self.destroy()

if __name__ == "__main__":
    app = MainApp()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()
