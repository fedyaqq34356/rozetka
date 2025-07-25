import argparse
import asyncio
import logging
import os
import re
import sqlite3
import sys
import time
import tempfile
from datetime import datetime, time
from typing import List, Dict, Optional

try:
    import cloudscraper
except ImportError:
    print("[ПОМИЛКА] Потрібно встановити cloudscraper: pip install cloudscraper")
    raise

try:
    import openpyxl
    from openpyxl.styles import NamedStyle, Font, PatternFill, Border, Side, Alignment
    from openpyxl.utils import get_column_letter
except ImportError:
    print("[ПОМИЛКА] Потрібно встановити openpyxl: pip install openpyxl")
    raise

try:
    from bs4 import BeautifulSoup
    _HAVE_BS4 = True
except ImportError:
    _HAVE_BS4 = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("[ПОПЕРЕДЖЕННЯ] Модуль python-dotenv не встановлено. Переконайтеся, що BOT_TOKEN задано як змінна оточення")

try:
    from aiogram import Bot, Dispatcher, F
    from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile
    from aiogram.filters import Command
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import StatesGroup, State
    from aiogram.fsm.storage.memory import MemoryStorage
    _HAVE_AIOGRAM = True
except ImportError:
    _HAVE_AIOGRAM = False
    print("[ПОПЕРЕДЖЕННЯ] Модуль aiogram не встановлено. Функціонал Telegram бота недоступний")


# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Константи
EXCEL_FILENAME = "rozetka_stock_history.xlsx"
EXCEL_FIELDS = ["name", "url", "category", "last_checked", "max_stock"]
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Визначення станів для FSM
class BotStates(StatesGroup):
    waiting_url = State()
    waiting_time = State()

class RozetkaStockChecker:
    def __init__(self, debug=False, delay=0.7):
        self.scraper = cloudscraper.create_scraper()
        self.base_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,uk;q=0.6',
            'Content-Type': 'application/json',
            'Origin': 'https://rozetka.com.ua',
            'Referer': 'https://rozetka.com.ua/',
            'X-Requested-With': 'XMLHttpRequest',
        }
        self.debug = debug
        self.delay = delay
        self.reset_session_state()

    def reset_session_state(self):
        self.csrf_token = None
        self.purchase_id = None
        self.scraper = cloudscraper.create_scraper()

    def get_csrf_token(self):
        try:
            resp = self.scraper.get('https://rozetka.com.ua/')
            cookies = self.scraper.cookies.get_dict()
            if self.debug:
                print("[ДЕБАГ] Кукі сайту:", cookies)
            self.csrf_token = cookies.get('_uss-csrf')
            if self.debug:
                print(f"[ДЕБАГ] Отримано CSRF токен: {self.csrf_token}")
            return self.csrf_token is not None
        except Exception as e:
            print(f"[CSRF] Помилка: {e}")
            return False

    @staticmethod
    def extract_product_id(url: str):
        match = re.search(r'/p(\d+)/', url)
        return int(match.group(1)) if match else None

    def _ensure_csrf(self):
        if not self.csrf_token:
            if not self.get_csrf_token():
                raise RuntimeError("Не вдалось отримати CSRF токен (_uss-csrf)")

    def clear_cart(self):
        try:
            if not self.csrf_token:
                return
            url = 'https://uss.rozetka.com.ua/session/cart-se/clear?country=UA&lang=ua'
            headers = self.base_headers.copy()
            headers['CSRF-Token'] = self.csrf_token
            r = self.scraper.post(url, json={}, headers=headers)
            if self.debug:
                print("[ДЕБАГ] clear_cart статус:", r.status_code)
                print("[ДЕБАГ] clear_cart тіло:", r.text[:300])
        except Exception as e:
            if self.debug:
                print(f"[clear_cart] Помилка: {e}")

    def add_to_cart(self, product_id):
        self._ensure_csrf()
        self.clear_cart()
        url = 'https://uss.rozetka.com.ua/session/cart-se/add?country=UA&lang=ua'
        headers = self.base_headers.copy()
        headers['CSRF-Token'] = self.csrf_token
        payload = [{"goods_id": product_id, "quantity": 1}]
        try:
            r = self.scraper.post(url, json=payload, headers=headers)
            if self.debug:
                print(f"[ДЕБАГ] add_to_cart статус для товару {product_id}:", r.status_code)
                print("[ДЕБАГ] add_to_cart тіло:", r.text[:500])
            if r.status_code == 200:
                data = r.json()
                goods_items = data.get('purchases', {}).get('goods')
                if goods_items and len(goods_items) > 0:
                    for item in goods_items:
                        if item.get('goods', {}).get('id') == product_id:
                            self.purchase_id = item['id']
                            if self.debug:
                                print(f"[ДЕБАГ] purchase_id встановлено: {self.purchase_id}")
                            return data
                    print(f"[ПОПЕРЕДЖЕННЯ] Товар {product_id} не знайдено в корзині")
                    return None
                else:
                    print("[ПОПЕРЕДЖЕННЯ] Порожня корзина після додавання")
                    return None
            return None
        except Exception as e:
            print(f"[add_to_cart] Помилка для товару {product_id}: {e}")
            return None

    def update_quantity(self, quantity):
        if not self.purchase_id or not self.csrf_token:
            if self.debug:
                print(f"[update_quantity] Відсутні дані: purchase_id={self.purchase_id}, csrf_token={bool(self.csrf_token)}")
            return None
        url = 'https://uss.rozetka.com.ua/session/cart-se/edit-quantity?country=UA&lang=ua'
        headers = self.base_headers.copy()
        headers['CSRF-Token'] = self.csrf_token
        payload = [{"purchase_id": self.purchase_id, "quantity": quantity}]
        try:
            r = self.scraper.post(url, json=payload, headers=headers)
            if self.debug:
                print(f"[ДЕБАГ] update_quantity({quantity}) статус:", r.status_code)
                print("[ДЕБАГ] update_quantity тіло:", r.text[:500])
            if r.status_code == 200:
                return r.json()
            return None
        except Exception as e:
            print(f"[update_quantity] Помилка: {e}")
            return None

    def binary_search_max_stock(self, product_id, max_attempts=100, upper_bound=10000):
        if self.debug:
            print(f"[БП] Починаємо бінарний пошук для товару {product_id}")
        add_data = self.add_to_cart(product_id)
        if not add_data:
            print(f"[БП] Не вдалося додати товар {product_id} до корзини")
            return None, None
        left, right = 1, upper_bound
        max_available = 0
        for attempt in range(max_attempts):
            if left > right:
                break
            mid = (left + right) // 2
            if self.debug:
                print(f"[БП] #{attempt+1} товар {product_id} -> тестуємо кількість {mid}")
            data = self.update_quantity(mid)
            if not data:
                if self.debug:
                    print(f"[БП] Не отримано відповіді на {mid}")
                break
            time.sleep(self.delay)
            errors = data.get('error_messages') or []
            not_enough = False
            for err in errors:
                if self.debug:
                    print(f"[БП] Помилка: {err}")
                if err.get('code') == 3002:
                    not_enough = True
                    break
            if not_enough:
                right = mid - 1
                if self.debug:
                    print(f"[БП] Недостатньо товару на {mid}, зменшуємо праву межу до {right}")
            else:
                max_available = mid
                left = mid + 1
                if self.debug:
                    print(f"[БП] {mid} товарів доступно, збільшуємо ліву межу до {left}")
        if self.debug:
            print(f"[БП] Результат для товару {product_id}: {max_available}")
        return max_available, add_data

    def parse_category_from_html(self, product_url, category_id):
        try:
            resp = self.scraper.get(product_url)
            html = resp.text
            if self.debug:
                print(f"[parse_category] Шукаємо категорію ID: {category_id}")
            if _HAVE_BS4:
                soup = BeautifulSoup(html, 'html.parser')
                selectors = [
                    f'a[href*="/c{category_id}/"]',
                    f'a[href*="/ua/c{category_id}/"]',
                    f'*[href*="c{category_id}"]',
                ]
                for selector in selectors:
                    elements = soup.select(selector)
                    for element in elements:
                        text = element.get_text(strip=True)
                        if text and len(text) > 2:
                            if self.debug:
                                print(f"[parse_category] Знайдено категорію: '{text}' за селектором '{selector}'")
                            return text
                breadcrumb_selectors = [
                    '.breadcrumbs a',
                    '.rz-breadcrumbs a',
                    '[data-testid="breadcrumbs"] a',
                    '.catalog-heading a'
                ]
                for selector in breadcrumb_selectors:
                    elements = soup.select(selector)
                    for element in elements:
                        href = element.get('href', '')
                        if f'/c{category_id}/' in href or f'c{category_id}' in href:
                            text = element.get_text(strip=True)
                            if text and len(text) > 2:
                                if self.debug:
                                    print(f"[parse_category] Знайдено в breadcrumbs: '{text}'")
                                return text
            patterns = [
                rf'<a[^>]+href="[^"]*/?c{category_id}/[^"]*"[^>]*>([^<]+)</a>',
                rf'<a[^>]+href="[^"]*c{category_id}[^"]*"[^>]*>(.*?)</a>',
                rf'href="[^"]*c{category_id}[^"]*"[^>]*>([^<]*)</a>',
            ]
            for pattern in patterns:
                matches = re.finditer(pattern, html, re.I | re.S)
                for match in matches:
                    text = re.sub(r'<[^>]+>', '', match.group(1)).strip()
                    text = re.sub(r'\s+', ' ', text)
                    if text and len(text) > 2:
                        if self.debug:
                            print(f"[parse_category] Знайдено regex: '{text}'")
                        return text
            if self.debug:
                print(f"[parse_category] Категорію з ID {category_id} не знайдено")
        except Exception as e:
            if self.debug:
                print(f"[parse_category] Помилка: {e}")
        return None

    def get_product_meta(self, product_url, add_data, product_id):
        title = None
        category_id = None
        original_url = product_url
        if add_data:
            goods_items = add_data.get('purchases', {}).get('goods')
            if goods_items:
                for item in goods_items:
                    goods = item.get('goods', {})
                    if goods.get('id') == product_id:
                        title = goods.get('title') or goods.get('name') or None
                        category_id = goods.get('category_id') or None
                        api_url = goods.get('href') or goods.get('url')
                        if api_url:
                            product_url = api_url
                        break
        if not title or not category_id:
            try:
                resp = self.scraper.get(original_url)
                html = resp.text
                if not title and _HAVE_BS4:
                    soup = BeautifulSoup(html, 'html.parser')
                    title_selectors = [
                        'h1.product__title',
                        'h1[data-testid="product-title"]',
                        '.product-title h1',
                        'h1.rz-product-title',
                        'h1'
                    ]
                    for selector in title_selectors:
                        element = soup.select_one(selector)
                        if element:
                            title = element.get_text(strip=True)
                            if title:
                                break
                if not category_id:
                    match = re.search(r'/c(\d+)/', product_url)
                    if not match:
                        match = re.search(r'/c(\d+)/', original_url)
                    if match:
                        category_id = int(match.group(1))
            except Exception as e:
                if self.debug:
                    print(f"[get_product_meta] Помилка парсингу HTML: {e}")
        category_name = None
        if category_id is not None:
            category_name = self.parse_category_from_html(product_url, category_id)
        if self.debug:
            print(f"[get_product_meta] Результат: title='{title}', category='{category_name}', category_id={category_id}")
        return title, category_name

    def check_product(self, product_url):
        self.reset_session_state()
        product_id = self.extract_product_id(product_url)
        if not product_id:
            return {"error": "Не вдалося витягти ID", "url": product_url}
        print(f"=== Перевіряємо товар ID {product_id}: {product_url}")
        max_stock, add_data = self.binary_search_max_stock(product_id)
        if max_stock is None:
            return {"error": "Не вдалося визначити кількість", "url": product_url, "product_id": product_id}
        title, category_name = self.get_product_meta(product_url, add_data, product_id)
        result = {
            "product_id": product_id,
            "url": product_url,
            "title": title or '',
            "category": category_name or '',
            "max_stock": max_stock,
        }
        print(f"✅ Результат для товару {product_id}: {max_stock} шт. | {title or 'Без назви'}")
        return result

class DatabaseManager:
    def __init__(self, db_path: str = "rozetka_bot.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                name TEXT,
                category TEXT,
                added_date DATE DEFAULT CURRENT_DATE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                check_date DATE,
                stock_count INTEGER,
                FOREIGN KEY (product_id) REFERENCES products (id),
                UNIQUE(product_id, check_date)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()
        conn.close()

    def add_product(self, url: str, name: str = "", category: str = "") -> bool:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO products (url, name, category, added_date) 
                VALUES (?, ?, ?, CURRENT_DATE)
            """, (url, name, category))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Помилка додавання товару: {e}")
            return False

    def get_product_id_by_url(self, url: str) -> Optional[int]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM products WHERE url = ?", (url,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def update_product_stock(self, product_id: int, stock_count: int):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                INSERT OR REPLACE INTO stock_history (product_id, check_date, stock_count) 
                VALUES (?, ?, ?)
            """, (product_id, today, stock_count))
            conn.commit()
            conn.close()
            logger.info(f"Обновлены остатки для товара {product_id}: {stock_count}")
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления остатков: {e}")
            return False

    def get_products(self) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.id, p.url, p.name, p.category,
                (SELECT stock_count FROM stock_history sh 
                    WHERE sh.product_id = p.id 
                    ORDER BY sh.check_date DESC LIMIT 1) as last_stock,
                (SELECT check_date FROM stock_history sh 
                    WHERE sh.product_id = p.id 
                    ORDER BY sh.check_date DESC LIMIT 1) as last_check
            FROM products p
            ORDER BY p.name
        """)
        products = []
        for row in cursor.fetchall():
            products.append({
                "id": row[0], 
                "url": row[1], 
                "name": row[2] or "Без названия", 
                "category": row[3] or "Без категории",
                "last_stock": row[4] or 0,
                "last_check": row[5] or "Никогда"
            })
        conn.close()
        return products

    def remove_product_by_id(self, product_id: int) -> bool:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM stock_history WHERE product_id = ?", (product_id,))
            cursor.execute("DELETE FROM products WHERE id = ?", (product_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Помилка видалення товару: {e}")
            return False

    def get_products_with_history(self) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT check_date FROM stock_history ORDER BY check_date")
        all_dates = [row[0] for row in cursor.fetchall()]
        cursor.execute("""
            SELECT p.id, p.name, p.url, p.category
            FROM products p
            ORDER BY p.name
        """)
        products_data = []
        for product_row in cursor.fetchall():
            product_id, name, url, category = product_row
            cursor.execute("""
                SELECT check_date, stock_count 
                FROM stock_history 
                WHERE product_id = ? 
                ORDER BY check_date
            """, (product_id,))
            history = dict(cursor.fetchall())
            product_data = {
                'name': name or 'Без названия',
                'url': url,
                'category': category or 'Без категории',
                'history': history,
                'all_dates': all_dates
            }
            products_data.append(product_data)
        conn.close()
        return products_data

    def get_product_by_id(self, product_id: int) -> Optional[Dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, url, name, category FROM products WHERE id = ?", (product_id,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return {"id": result[0], "url": result[1], "name": result[2], "category": result[3]}
        return None

    def get_schedule_time(self) -> Optional[str]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = 'schedule_time'")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def set_schedule_time(self, time_str: str):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('schedule_time', ?)", (time_str,))
        conn.commit()
        conn.close()

    def sync_with_excel(self):
        try:
            if os.path.exists(EXCEL_FILENAME):
                df = load_existing_excel(EXCEL_FILENAME)
                logger.info(f"Завантажено {len(df)} записів з Excel")
                for row in df:
                    if row.get('url'):
                        self.add_product(
                            url=str(row.get('url', '')),
                            name=str(row.get('name', '')),
                            category=str(row.get('category', ''))
                        )
                        if row.get('max_stock') is not None:
                            product_id = self.get_product_id_by_url(str(row.get('url', '')))
                            if product_id:
                                stock = int(row.get('max_stock', 0))
                                self.update_product_stock(product_id, stock)
                logger.info("Синхронізація з Excel завершена")
        except Exception as e:
            logger.error(f"Помилка синхронізації з Excel: {e}")

    def export_to_excel(self):
        try:
            products = self.get_products()
            excel_data = []
            for product in products:
                excel_data.append({
                    'name': product['name'],
                    'url': product['url'],
                    'category': product['category'],
                    'last_checked': product['last_check'],
                    'max_stock': product['last_stock']
                })
            if excel_data:
                existing_df = load_existing_excel(EXCEL_FILENAME)
                updated_df = upsert_rows(existing_df, excel_data)
                save_excel_with_formatting(EXCEL_FILENAME, updated_df)
                logger.info(f"Експортовано {len(excel_data)} товарів в Excel")
        except Exception as e:
            logger.error(f"Помилка експорту в Excel: {e}")

def load_existing_excel(path: str):
    if not os.path.exists(path):
        return []
    try:
        from openpyxl import load_workbook
        workbook = load_workbook(path, read_only=True)
        worksheet = workbook.active
        headers = []
        for cell in worksheet[1]:
            if cell.value:
                headers.append(cell.value)
            else:
                break
        if not headers:
            return []
        data = []
        for row in worksheet.iter_rows(min_row=2, values_only=True):
            if not any(row):
                continue
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(headers):
                    row_dict[headers[i]] = value if value is not None else ''
            for field in EXCEL_FIELDS:
                if field not in row_dict:
                    row_dict[field] = ''
            data.append(row_dict)
        workbook.close()
        return data
    except Exception as e:
        print(f"[ПОПЕРЕДЖЕННЯ] Не вдалося завантажити існуючий Excel файл: {e}")
        print("Створюємо новий файл...")
        return []

def save_excel_with_formatting(path: str, data_list):
    if not data_list:
        print("[ПОПЕРЕДЖЕННЯ] Список даних порожній, створюємо файл тільки з заголовками")
        data_list = []
    products_history = {}
    all_dates = set()
    for row in data_list:
        url = row.get('url', '')
        if url not in products_history:
            products_history[url] = {
                'name': row.get('name', ''),
                'category': row.get('category', ''),
                'url': url,
                'dates': {}
            }
        date = row.get('last_checked', '')
        if date:
            date_only = date.split(' ')[0] if ' ' in date else date
            products_history[url]['dates'][date_only] = row.get('max_stock', 0)
            all_dates.add(date_only)
    sorted_dates = sorted(list(all_dates))
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Істория залишків"
    headers = ["Назва", "URL", "Категорія"] + sorted_dates
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num, value=header)
        cell.font = Font(name='Arial', size=12, bold=True, color='FFFFFF')
        cell.fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
    row_num = 2
    for product_data in products_history.values():
        cell = ws.cell(row=row_num, column=1, value=product_data['name'])
        cell.font = Font(name='Arial', size=11)
        cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        cell = ws.cell(row=row_num, column=2, value=product_data['url'])
        cell.font = Font(name='Arial', size=11)
        cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        cell = ws.cell(row=row_num, column=3, value=product_data['category'])
        cell.font = Font(name='Arial', size=11)
        cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)
        cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        for col_idx, date in enumerate(sorted_dates, 4):
            stock_value = product_data['dates'].get(date, '')
            cell = ws.cell(row=row_num, column=col_idx, value=stock_value)
            cell.font = Font(name='Arial', size=11)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
            if stock_value and stock_value > 0:
                cell.fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
            elif stock_value == 0:
                cell.fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
            if row_num % 2 == 0:
                if not cell.fill.start_color or cell.fill.start_color.rgb == '00000000':
                    cell.fill = PatternFill(start_color='F2F2F2', end_color='F2F2F2', fill_type='solid')
        row_num += 1
    ws.column_dimensions['A'].width = 40
    ws.column_dimensions['B'].width = 60
    ws.column_dimensions['C'].width = 25
    for col_idx in range(4, len(headers) + 1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = 12
    for row in ws.iter_rows():
        ws.row_dimensions[row[0].row].height = 25
    ws.freeze_panes = 'A2'
    max_row = len(products_history) + 1
    max_col = len(headers)
    ws.auto_filter.ref = f"A1:{get_column_letter(max_col)}{max_row}"
    try:
        wb.save(path)
    except Exception as e:
        print(f"[ОШИБКА] Не удалось сохранить Excel файл: {e}")
        raise

def upsert_rows(existing_data, new_items):
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_rows = []
    for item in new_items:
        if 'error' in item:
            print(f"[ПОПЕРЕДЖЕННЯ] {item.get('url', 'Невідомий URL')}: {item['error']}")
            continue
        new_rows.append({
            'name': item.get('title', ''),
            'url': item.get('url', ''),
            'category': item.get('category', ''),
            'last_checked': now_str,
            'max_stock': item.get('max_stock', 0),
        })
    if not existing_data:
        existing_data = []
    new_urls = [row['url'] for row in new_rows]
    filtered_existing = [row for row in existing_data if row.get('url', '') not in new_urls]
    filtered_existing.extend(new_rows)
    return filtered_existing

def read_urls_from_file(fname):
    urls = []
    with open(fname, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            urls.append(line)
    return urls

def get_interactive_urls():
    if not sys.stdin.isatty():
        print("❌ Интерактивный режим недоступен в неинтерактивной среде")
        print("💡 Используйте аргументы командной строки или файл с URL")
        return []
    print("\n" + "="*70)
    print("🛒 ROZETKA STOCK CHECKER - Інтерактивний режим")
    print("="*70)
    print("📝 Введіть URL товарів для перевірки залишків:")
    print("   • Вводьте по одному URL в рядку")
    print("   • Для завершення натисніть Enter на порожньому рядку")
    print("   • Для виходу введіть 'exit' або 'quit'")
    print("-"*70)
    urls = []
    counter = 1
    while True:
        try:
            url = input(f"🔗 URL №{counter}: ").strip()
            if not url:
                if urls:
                    print(f"\n✅ Введено {len(urls)} URL(s). Починаємо перевірку...")
                    break
                else:
                    print("❌ Не введено жодного URL. Спробуйте ще раз.")
                    continue
            if url.lower() in ['exit', 'quit', 'вихід']:
                print("👋 Вихід з програми...")
                sys.exit(0)
            if url.startswith('http') and 'rozetka.com.ua' in url:
                urls.append(url)
                print(f"   ✓ URL №{counter} додано")
                counter += 1
            else:
                print("   ❌ URL має починатися з http:// або https:// та містити rozetka.com.ua")
        except KeyboardInterrupt:
            print("\n\n👋 Програма перервана користувачем")
            sys.exit(0)
        except EOFError:
            print("\n❌ Помилка вводу. Завершення роботи.")
            break
    return urls

class RozetkaTelegramBot:
    def __init__(self):
        if not _HAVE_AIOGRAM:
            raise ImportError("Модуль aiogram не встановлено. Функціонал Telegram бота недоступний")
        if not BOT_TOKEN:
            raise ValueError("BOT_TOKEN не задано в змінних оточення")
        self.bot = Bot(token=BOT_TOKEN)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.db = DatabaseManager()
        self.checker = RozetkaStockChecker(debug=False, delay=0.7)
        self.setup_handlers()
        self.db.sync_with_excel()

    def setup_handlers(self):
        self.dp.message(Command("start"))(self.cmd_start)
        self.dp.message(Command("help"))(self.cmd_help)
        self.dp.message(Command("add"))(self.cmd_add_url)
        self.dp.message(Command("list"))(self.cmd_list_products)
        self.dp.message(Command("remove"))(self.cmd_remove_product)
        self.dp.message(Command("schedule"))(self.cmd_set_schedule)
        self.dp.message(Command("check"))(self.cmd_manual_check)
        self.dp.message(Command("export"))(self.cmd_export_table)
        self.dp.message(Command("sync"))(self.cmd_sync_excel)
        self.dp.message(F.text)(self.handle_text_messages)
        self.dp.callback_query()(self.handle_callback_query)

    async def cmd_sync_excel(self, message: Message):
        await message.reply("🔄 Синхронізую з Excel файлом...")
        try:
            self.db.sync_with_excel()
            products_count = len(self.db.get_products())
            await message.reply(f"✅ Синхронізація завершена!\n📊 Всього товарів: {products_count}")
        except Exception as e:
            await message.reply(f"❌ Помилка синхронізації: {str(e)}")

    async def cmd_start(self, message: Message):
        await message.reply(
            "🛒 <b>Бот перевірки залишків Rozetka</b>\n\n"
            "📋 Доступні команди:\n"
            "/add - додати товар\n"
            "/list - список товарів\n"
            "/remove - видалити товар\n"
            "/schedule - налаштувати розклад\n"
            "/check - ручна перевірка\n"
            "/export - експорт таблиці\n"
            "/sync - синхронізація з Excel\n"
            "/help - допомога",
            parse_mode="HTML"
        )

    async def cmd_help(self, message: Message):
        await message.reply(
            "🔧 <b>Інструкція:</b>\n\n"
            "1. Додайте товари командою /add\n"
            "2. Встановіть час перевірки /schedule\n"
            "3. Бот щодня перевірятиме залишки\n"
            "4. Експортуйте дані /export\n"
            "5. Синхронізуйте з Excel /sync\n\n"
            "⚠️ Формат часу: ГГ:ХХ (наприклад, 09:30)",
            parse_mode="HTML"
        )

    async def cmd_add_url(self, message: Message, state: FSMContext):
        await state.set_state(BotStates.waiting_url)
        await message.reply("🔗 Надішліть посилання на товар Rozetka:")

    async def cmd_list_products(self, message: Message):
        products = self.db.get_products()
        if not products:
            await message.reply("📦 Список товарів порожній")
            return
        text = "📋 <b>Список товарів:</b>\n\n"
        for i, product in enumerate(products, 1):
            name = product['name']
            category = product['category']
            stock = product['last_stock']
            last_check = product['last_check']
            text += f"{i}. <b>{name}</b>\n"
            text += f"   📂 {category}\n"
            text += f"   📊 Залишки: {stock}\n"
            text += f"   🕐 Остання перевірка: {last_check}\n"
            text += f"   🔗 {product['url'][:50]}...\n\n"
        await message.reply(text, parse_mode="HTML")

    async def cmd_remove_product(self, message: Message):
        products = self.db.get_products()
        if not products:
            await message.reply("📦 Немає товарів для видалення")
            return
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🗑 {p['name'][:30]}...", 
                                 callback_data=f"remove_{p['id']}")] for p in products
        ])
        await message.reply("Оберіть товар для видалення:", reply_markup=keyboard)

    async def cmd_set_schedule(self, message: Message, state: FSMContext):
        await state.set_state(BotStates.waiting_time)
        current_time = self.db.get_schedule_time()
        text = "🕐 Введіть час щоденної перевірки (формат ГГ:ХХ):"
        if current_time:
            text += f"\n\n⏰ Поточний час: {current_time}"
        await message.reply(text)

    async def cmd_manual_check(self, message: Message):
        await message.reply("🔍 Запускаю ручну перевірку залишків...")
        results = await self.check_products_without_saving()
        if results:
            report = "✅ <b>Ручна перевірка завершена!</b>\n\n📊 <b>Результати:</b>\n\n"
            for result in results:
                report += f"📦 <b>{result['name']}</b>\n"
                if result['success']:
                    report += f"   📈 Залишки: {result['stock']}\n"
                else:
                    report += f"   ❌ Помилка: {result['error']}\n"
                report += "\n"
            report += "ℹ️ <i>Дані НЕ збережено в історію. Збереження тільки при автоматичній перевірці.</i>"
            if len(report) > 4000:
                await message.reply("✅ Перевірка завершена! Результати надсилаються частинами...")
                chunks = [report[i:i+4000] for i in range(0, len(report), 4000)]
                for chunk in chunks:
                    await message.reply(chunk, parse_mode="HTML")
            else:
                await message.reply(report, parse_mode="HTML")
        else:
            await message.reply("✅ Перевірка завершена, але товарів для перевірки немає")

    async def cmd_export_table(self, message: Message):
        await message.reply("📊 Генерую Excel таблицю...")
        try:
            self.db.export_to_excel()
            products = self.db.get_products()
            if not products:
                await message.reply("❌ Немає товарів для експорту")
                return
            excel_path = await self.generate_excel()
            if not os.path.exists(excel_path):
                await message.reply("❌ Помилка створення файлу")
                return
            file_size = os.path.getsize(excel_path)
            if file_size == 0:
                await message.reply("❌ Створений файл порожній")
                os.remove(excel_path)
                return
            await message.reply_document(
                document=FSInputFile(excel_path, filename="rozetka_stock_history.xlsx"),
                caption=f"📋 Таблиця залишків Rozetka\n📊 Товарів: {len(products)}\n📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            )
            os.remove(excel_path)
            logger.info(f"Експорт виконано для {len(products)} товарів")
        except Exception as e:
            logger.error(f"Помилка експорту: {e}")
            await message.reply(f"❌ Помилка створення таблиці: {str(e)}")

    async def check_products_without_saving(self) -> List[Dict]:
        products = self.db.get_products()
        results = []
        for i, product in enumerate(products, 1):
            try:
                logger.info(f"Ручна перевірка товару {i}/{len(products)}: {product['name']}")
                result = self.checker.check_product(product['url'])
                if 'error' not in result:
                    stock_count = result.get('max_stock', 0)
                    product_name = result.get('title', product['name'])
                    results.append({
                        'name': product_name or 'Без назви',
                        'success': True,
                        'stock': stock_count
                    })
                    logger.info(f"Ручна перевірка - Успіх: {product_name}, залишки: {stock_count}")
                else:
                    results.append({
                        'name': product['name'],
                        'success': False,
                        'error': result['error']
                    })
                    logger.error(f"Ручна перевірка - Помилка для товару {product['url']}: {result['error']}")
            except Exception as e:
                logger.error(f"Критична помилка ручної перевірки товару {product['url']}: {e}")
                results.append({
                    'name': product['name'],
                    'success': False,
                    'error': str(e)
                })
            if i < len(products):
                await asyncio.sleep(2)
        return results

    async def generate_excel(self) -> str:
        temp_dir = tempfile.gettempdir()
        filename = f"rozetka_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(temp_dir, filename)
        try:
            products_data = self.db.get_products_with_history()
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Історія залишків"
            all_dates = set()
            for product in products_data:
                all_dates.update(product['all_dates'])
            sorted_dates = sorted(list(all_dates))
            headers = ["Товар", "URL", "Категорія"]
            for date in sorted_dates:
                headers.extend([f"{date}\nкількість", f"{date}\nзміни"])
            for col, header in enumerate(headers, 1):
                cell = ws.cell(row=1, column=col, value=header)
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                cell.border = Border(
                    left=Side(style='thin'),
                    right=Side(style='thin'),
                    top=Side(style='thin'),
                    bottom=Side(style='thin')
                )
            for row_idx, product in enumerate(products_data, 2):
                ws.cell(row=row_idx, column=1, value=product['name'])
                ws.cell(row=row_idx, column=2, value=product['url'])
                ws.cell(row=row_idx, column=3, value=product['category'])
                previous_stock = None
                col_idx = 4
                for date in sorted_dates:
                    current_stock = product['history'].get(date, '')
                    stock_cell = ws.cell(row=row_idx, column=col_idx, value=current_stock)
                    stock_cell.alignment = Alignment(horizontal="center", vertical="center")
                    stock_cell.border = Border(
                        left=Side(style='thin'),
                        right=Side(style='thin'),
                        top=Side(style='thin'),
                        bottom=Side(style='thin')
                    )
                    if current_stock and current_stock > 0:
                        stock_cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                    elif current_stock == 0:
                        stock_cell.fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
                    change_cell = ws.cell(row=row_idx, column=col_idx + 1)
                    change_cell.alignment = Alignment(horizontal="center", vertical="center")
                    change_cell.border = Border(
                        left=Side(style='thin'),
                        right=Side(style='thin'),
                        top=Side(style='thin'),
                        bottom=Side(style='thin')
                    )
                    if previous_stock is not None and current_stock != '' and previous_stock != '':
                        try:
                            change = int(current_stock) - int(previous_stock)
                            if change != 0:
                                change_cell.value = change
                                if change > 0:
                                    change_cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                                    change_cell.font = Font(color="006100", bold=True)
                                else:
                                    change_cell.fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
                                    change_cell.font = Font(color="9C0006", bold=True)
                        except (ValueError, TypeError):
                            pass
                    if current_stock != '':
                        previous_stock = current_stock
                    col_idx += 2
            ws.column_dimensions['A'].width = 40
            ws.column_dimensions['B'].width = 60
            ws.column_dimensions['C'].width = 25
            col_idx = 4
            for _ in sorted_dates:
                col_letter_qty = get_column_letter(col_idx)
                col_letter_change = get_column_letter(col_idx + 1)
                ws.column_dimensions[col_letter_qty].width = 12
                ws.column_dimensions[col_letter_change].width = 10
                col_idx += 2
            ws.row_dimensions[1].height = 30
            for row in range(2, len(products_data) + 2):
                ws.row_dimensions[row].height = 25
            ws.freeze_panes = 'D2'
            max_row = len(products_data) + 1
            max_col = len(headers)
            ws.auto_filter.ref = f"A1:{get_column_letter(max_col)}{max_row}"
            wb.save(filepath)
            logger.info(f"Excel файл створено: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Помилка створення Excel: {e}")
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Помилка"
            ws.cell(row=1, column=1, value=f"Помилка створення файлу: {str(e)}")
            wb.save(filepath)
            return filepath

    async def handle_callback_query(self, callback_query: CallbackQuery):
        if callback_query.data.startswith("remove_"):
            product_id = int(callback_query.data.split("_")[1])
            product = self.db.get_product_by_id(product_id)
            if product:
                success = self.db.remove_product_by_id(product_id)
                if success:
                    self.db.export_to_excel()
                    await callback_query.message.edit_text(
                        f"✅ Товар успішно видалено!\n\n"
                        f"📦 <b>{product['name'] or 'Без назви'}</b>\n"
                        f"🔗 {product['url'][:50]}...",
                        parse_mode="HTML"
                    )
                else:
                    await callback_query.message.edit_text("❌ Помилка видалення товару")
            else:
                await callback_query.message.edit_text("❌ Товар не знайдено")
        await callback_query.answer()

    async def handle_text_messages(self, message: Message, state: FSMContext):
        current_state = await state.get_state()
        if current_state == BotStates.waiting_url:
            await self.process_url(message, state)
        elif current_state == BotStates.waiting_time:
            await self.process_schedule_time(message, state)

    async def process_url(self, message: Message, state: FSMContext):
        url = message.text.strip()
        if not re.match(r'https?://.*rozetka\.com\.ua.*', url):
            await message.reply("❌ Невірне посилання. Потрібно посилання на rozetka.com.ua")
            return
        processing_msg = await message.reply("⏳ Обробляю товар...")
        try:
            result = self.checker.check_product(url)
            if 'error' in result:
                await processing_msg.edit_text(f"❌ Помилка: {result['error']}")
                return
            success = self.db.add_product(
                url=result['url'],
                name=result.get('title', ''),
                category=result.get('category', '')
            )
            if success:
                stock = result.get('max_stock', 0)
                success_text = (
                    f"✅ Товар додано!\n\n"
                    f"📦 <b>{result.get('title', 'Без назви')}</b>\n"
                    f"📂 Категорія: {result.get('category', 'Невідома')}\n"
                    f"📊 Поточні залишки: {stock}\n"
                    f"🔗 URL: {result['url'][:50]}...\n\n"
                    f"ℹ️ Залишки будуть збережені тільки при автоматичній перевірці"
                )
                await processing_msg.edit_text(success_text, parse_mode="HTML")
            else:
                await processing_msg.edit_text("❌ Помилка збереження товару")
        except Exception as e:
            logger.error(f"Помилка обробки URL {url}: {e}")
            await processing_msg.edit_text(f"❌ Помилка обробки: {str(e)}")
        await state.clear()

    async def process_schedule_time(self, message: Message, state: FSMContext):
        time_text = message.text.strip()
        if not re.match(r'^\d{1,2}:\d{2}$', time_text):
            await message.reply("❌ Неправильний формат часу. Використовуйте ГГ:ХХ (наприклад, 09:30)")
            return
        try:
            time.fromisoformat(time_text + ":00")
            self.db.set_schedule_time(time_text)
            await message.reply(f"✅ Час щоденної перевірки встановлено: {time_text}")
        except ValueError:
            await message.reply("❌ Неправильний час. Використовуйте формат ГГ:ХХ")
        await state.clear()

    async def check_all_products(self, manual=False) -> List[Dict]:
        products = self.db.get_products()
        results = []
        for i, product in enumerate(products, 1):
            try:
                logger.info(f"Перевіряю товар {i}/{len(products)}: {product['name']}")
                result = self.checker.check_product(product['url'])
                if 'error' not in result:
                    updated_name = result.get('title', product['name'])
                    updated_category = result.get('category', product['category'])
                    if updated_name != product['name'] or updated_category != product['category']:
                        self.db.add_product(
                            product['url'],
                            updated_name,
                            updated_category
                        )
                    stock_count = result.get('max_stock', 0)
                    if not manual:
                        self.db.update_product_stock(product['id'], stock_count)
                    results.append({
                        'name': updated_name or 'Без назви',
                        'success': True,
                        'stock': stock_count
                    })
                    logger.info(f"Успіх: {updated_name}, залишки: {stock_count}")
                else:
                    results.append({
                        'name': product['name'],
                        'success': False,
                        'error': result['error']
                    })
                    logger.error(f"Помилка для товару {product['url']}: {result['error']}")
            except Exception as e:
                logger.error(f"Критична помилка перевірки товару {product['url']}: {e}")
                results.append({
                    'name': product['name'],
                    'success': False,
                    'error': str(e)
                })
            if i < len(products):
                await asyncio.sleep(2)
        return results

    async def schedule_checker(self):
        while True:
            try:
                schedule_time = self.db.get_schedule_time()
                if schedule_time:
                    now = datetime.now().time()
                    target_time = time.fromisoformat(schedule_time + ":00")
                    if now.hour == target_time.hour and now.minute == target_time.minute:
                        logger.info("Запуск планової АВТОМАТИЧНОЇ перевірки")
                        await self.check_all_products()
                        self.db.export_to_excel()
                        await asyncio.sleep(60)
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Помилка планувальника: {e}")
                await asyncio.sleep(300)

    async def start_bot(self):
        logger.info("Запуск Telegram бота")
        asyncio.create_task(self.schedule_checker())
        await self.dp.start_polling(self.bot)

def parse_cli():
    p = argparse.ArgumentParser(description="Rozetka stock checker -> Excel таблиця")
    p.add_argument('urls', nargs='*', help='URL товарів')
    p.add_argument('-f', '--file', help='Файл зі списком URL (по 1 в рядку)')
    p.add_argument('--interactive', action='store_true', help='Інтерактивний режим для вводу URL')
    p.add_argument('--debug', action='store_true', help='Дебаг вивід')
    p.add_argument('--delay', type=float, default=0.7, help='Затримка між запитами під час бінарного пошуку')
    p.add_argument('--bot', action='store_true', help='Запустити Telegram бот')
    return p.parse_args()

def main():
    args = parse_cli()
    if args.bot and _HAVE_AIOGRAM:
        print("🚀 Запуск Telegram бота...")
        bot_instance = RozetkaTelegramBot()
        asyncio.run(bot_instance.start_bot())
    else:
        if args.bot and not _HAVE_AIOGRAM:
            print("❌ Модуль aiogram не встановлено. Неможливо запустити Telegram бот.")
            return
        print("🚀 Запуск Rozetka Stock Checker...")
        urls = list(args.urls)
        if args.file:
            print(f"📄 Завантажуємо URL з файлу: {args.file}")
            urls.extend(read_urls_from_file(args.file))
        if not urls and args.interactive:
            print("🔄 Запускається інтерактивний режим...")
            urls = get_interactive_urls()
        if not urls:
            print("❌ Не знайдено URL для перевірки!")
            return
        print(f"\n🎯 Знайдено {len(urls)} товарів для перевірки")
        print("⏳ Починаємо перевірку залишків...\n")
        checker = RozetkaStockChecker(debug=args.debug, delay=args.delay)
        results = []
        for i, url in enumerate(urls, 1):
            print(f"[{i}/{len(urls)}] Перевіряємо товар...")
            res = checker.check_product(url)
            results.append(res)
            if i < len(urls):
                print("⏱️ Пауза між запитами...")
                time.sleep(2)
        existing = load_existing_excel(EXCEL_FILENAME)
        merged = upsert_rows(existing, results)
        save_excel_with_formatting(EXCEL_FILENAME, merged)
        print("\n" + "="*70)
        print("✅ ГОТОВО! Результати перевірки:")
        print("="*70)
        print(f"📊 Файл збережено: {os.path.abspath(EXCEL_FILENAME)}")
        print(f"📈 Всього записів в таблиці: {len(merged)}")
        print("-"*70)
        success_count = 0
        for item in results:
            if 'error' in item:
                print(f"❌ ПОМИЛКА: {item['url']} - {item['error']}")
            else:
                success_count += 1
                print(f"✅ {item['title']}")
                print(f"   📂 Категорія: {item['category']}")
                print(f"   📦 Максимальна кількість: {item['max_stock']}")
                print(f"   🔗 URL: {item['url'][:60]}...")
                print()
        print("="*70)
        print(f"🎉 Успішно оброблено: {success_count}/{len(results)} товарів")

if __name__ == "__main__":
    main()
