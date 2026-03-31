import sqlite3
import uuid
from datetime import datetime
import os

# Đặt file DB ở thư mục /data của dự án
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'chat_sessions.db')

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS sessions (
                            id TEXT PRIMARY KEY,
                            title TEXT,
                            updated_at DATETIME)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS messages (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            session_id TEXT,
                            role TEXT,
                            content TEXT,
                            cv_text TEXT,
                            created_at DATETIME)''')

def get_all_sessions():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute('SELECT * FROM sessions ORDER BY updated_at DESC')
        return [dict(row) for row in cur.fetchall()]

def create_session(title="Chat mới"):
    session_id = str(uuid.uuid4())
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('INSERT INTO sessions (id, title, updated_at) VALUES (?, ?, ?)',
                     (session_id, title, datetime.now()))
    return session_id

def get_session_messages(session_id):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute('SELECT * FROM messages WHERE session_id = ? ORDER BY id ASC', (session_id,))
        return [dict(row) for row in cur.fetchall()]

def add_message(session_id, role, content, cv_text=""):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('INSERT INTO messages (session_id, role, content, cv_text, created_at) VALUES (?, ?, ?, ?, ?)',
                     (session_id, role, content, cv_text, datetime.now()))
        conn.execute('UPDATE sessions SET updated_at = ? WHERE id = ?', (datetime.now(), session_id))

def delete_session(session_id):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('DELETE FROM messages WHERE session_id = ?', (session_id,))
        conn.execute('DELETE FROM sessions WHERE id = ?', (session_id,))