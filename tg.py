import argparse
import os
import re
import sys
import time
from datetime import datetime

try:
    import cloudscraper
except ImportError:
    print("[ПОМИЛКА] Потрібно встановити cloudscraper: pip install cloudscraper")
    raise

try:
    import pandas as pd
except ImportError:
    print("[ПОМИЛКА] Потрібно встановити pandas: pip install pandas")
    raise

try:
    import openpyxl
    from openpyxl.styles import NamedStyle, Font, PatternFill, Border, Side, Alignment
    from openpyxl.utils.dataframe import dataframe_to_rows
except ImportError:
    print("[ПОМИЛКА] Потрібно встановити openpyxl: pip install openpyxl")
    raise

try:
    from bs4 import BeautifulSoup
    _HAVE_BS4 = True
except ImportError:
    print("[ПОМИЛКА] Потрібно встановити beautifulsoup4: pip install beautifulsoup4")
    _HAVE_BS4 = False


class RozetkaStockChecker:
    def __init__(self, debug=False, delay=0.7):
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
            interpreter='js2py'
        )
        self.base_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,uk;q=0.6',
            'Origin': 'https://rozetka.com.ua',
            'Referer': 'https://rozetka.com.ua/',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Dest': 'document',
            'Upgrade-Insecure-Requests': '1',
            'Accept-Encoding': 'gzip, deflate, br'
        }
        self.debug = debug
        self.delay = delay
        self.reset_session_state()

    def reset_session_state(self):
        """Очищаем состояние сессии перед проверкой нового товара"""
        self.csrf_token = None
        self.purchase_id = None
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
            interpreter='js2py'
        )

    def get_csrf_token(self):
        try:
            resp = self.scraper.get('https://rozetka.com.ua/', headers=self.base_headers, timeout=15)
            resp.raise_for_status()
            cookies = self.scraper.cookies.get_dict()
            if self.debug:
                print("[ДЕБАГ] Куки сайта:", cookies)
            self.csrf_token = cookies.get('_uss-csrf')
            if self.debug:
                print(f"[ДЕБАГ] Отримано CSRF токен: {self.csrf_token}")
            return self.csrf_token is not None
        except Exception as e:
            if self.debug:
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
            r = self.scraper.post(url, json={}, headers=headers, timeout=15)
            if self.debug:
                print("[ДЕБАГ] clear_cart статус:", r.status_code)
                print("[ДЕБАГ] clear_cart тело:", r.text[:300])
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
            r = self.scraper.post(url, json=payload, headers=headers, timeout=15)
            if self.debug:
                print(f"[ДЕБАГ] add_to_cart статус для товара {product_id}:", r.status_code)
                print("[ДЕБАГ] add_to_cart тело:", r.text[:500])
            if r.status_code == 200:
                data = r.json()
                goods_items = data.get('purchases', {}).get('goods', [])
                for item in goods_items:
                    if item.get('goods', {}).get('id') == product_id:
                        self.purchase_id = item['id']
                        if self.debug:
                            print(f"[ДЕБАГ] purchase_id установлено: {self.purchase_id}")
                        return data
                if self.debug:
                    print(f"[ПОПЕРЕДЖЕННЯ] Товар {product_id} не найден в корзине")
                return None
            return None
        except Exception as e:
            if self.debug:
                print(f"[add_to_cart] Помилка для товара {product_id}: {e}")
            return None

    def update_quantity(self, quantity):
        if not self.purchase_id or not self.csrf_token:
            if self.debug:
                print(f"[update_quantity] Отсутствуют данные: purchase_id={self.purchase_id}, csrf_token={bool(self.csrf_token)}")
            return None
        url = 'https://uss.rozetka.com.ua/session/cart-se/edit-quantity?country=UA&lang=ua'
        headers = self.base_headers.copy()
        headers['CSRF-Token'] = self.csrf_token
        payload = [{"purchase_id": self.purchase_id, "quantity": quantity}]
        try:
            r = self.scraper.post(url, json=payload, headers=headers, timeout=15)
            if self.debug:
                print(f"[ДЕБАГ] update_quantity({quantity}) статус:", r.status_code)
                print("[ДЕБАГ] update_quantity тело:", r.text[:500])
            if r.status_code == 200:
                return r.json()
            return None
        except Exception as e:
            if self.debug:
                print(f"[update_quantity] Помилка: {e}")
            return None

    def binary_search_max_stock(self, product_id, max_attempts=100, upper_bound=10000):
        if self.debug:
            print(f"[БП] Починаємо бінарний пошук для товару {product_id}")
        add_data = self.add_to_cart(product_id)
        if not add_data:
            if self.debug:
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

    def get_category_from_api(self, category_id):
        """Попытка получить название категории через API"""
        try:
            url = f'https://rozetka.com.ua/api/v2/categories/{category_id}?lang=ua'
            headers = self.base_headers.copy()
            resp = self.scraper.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                category_name = data.get('data', {}).get('title') or data.get('data', {}).get('name')
                if category_name and 2 < len(category_name) < 100:
                    if self.debug:
                        print(f"[get_category_from_api] Знайдено через API: '{category_name}'")
                    return category_name
            if self.debug:
                print(f"[get_category_from_api] API не вернуло категорию для ID {category_id}")
            return None
        except Exception as e:
            if self.debug:
                print(f"[get_category_from_api] Помилка API: {e}")
            return None

    def parse_category_from_html(self, product_url, category_id):
        """Парсинг категории с приоритетом на rz-breadcrumbs и a[rzrelnofollow].black-link"""
        try:
            time.sleep(1)  # Задержка для обхода Cloudflare
            resp = self.scraper.get(product_url, headers=self.base_headers, timeout=15)
            resp.raise_for_status()
            html = resp.text

            if self.debug:
                with open("debug_page.html", "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"[parse_category] HTML збережено в debug_page.html для URL: {product_url}")

            if self.debug:
                print(f"[parse_category] Шукаємо категорію ID: {category_id}")

            if _HAVE_BS4:
                soup = BeautifulSoup(html, 'html.parser')

                # Приоритетный селектор для XPath
                xpath_selector = 'rz-breadcrumbs div:nth-child(6) a'
                link = soup.select_one(xpath_selector)
                if link:
                    text = link.get_text(strip=True)
                    if text and 2 < len(text) < 100 and not any(
                        skip in text.lower() for skip in ['>', '<', 'img', 'svg', 'icon', 'span']
                    ):
                        if self.debug:
                            print(f"[parse_category] Знайдено в селекторі '{xpath_selector}': '{text}'")
                        return text

                # Дополнительные селекторы
                selectors = [
                    'a[rzrelnofollow].black-link',
                    'a.black-link[rzrelnofollow]',
                    'a.d-flex.black-link',
                    'a[rzrelnofollow][class*="black-link"]',
                    f'a[href*="/c{category_id}/"]',
                    f'a[href*="/ua/c{category_id}/"]',
                    '.breadcrumbs a',
                    '.rz-breadcrumbs a',
                    '[data-testid="breadcrumbs"] a',
                    '.catalog-heading a'
                ]

                for selector in selectors:
                    try:
                        link = soup.select_one(selector)
                        if link:
                            href = link.get('href', '')
                            if f'/c{category_id}/' in href or f'c{category_id}' in href:
                                text = link.get_text(strip=True)
                                if text and 2 < len(text) < 100 and not any(
                                    skip in text.lower() for skip in ['>', '<', 'img', 'svg', 'icon', 'span']
                                ):
                                    if self.debug:
                                        print(f"[parse_category] Знайдено в селекторі '{selector}': '{text}'")
                                    return text
                    except Exception as e:
                        if self.debug:
                            print(f"[parse_category] Помилка селектора '{selector}': {e}")
                        continue

            # Резервный поиск через regex
            regex_patterns = [
                rf'<a[^>]+href="[^"]*/?c{category_id}/[^"]*"[^>]*>([^<]+)</a>',
                rf'<a[^>]+href="[^"]*c{category_id}[^"]*"[^>]*>([^<]*?)</a>',
                rf'"name"\s*:\s*"([^"]*)"[^}}]*"categoryId"\s*:\s*"?{category_id}"?',
                rf'"title"\s*:\s*"([^"]*)"[^}}]*"id"\s*:\s*"?{category_id}"?'
            ]

            for pattern in regex_patterns:
                try:
                    matches = re.finditer(pattern, html, re.I | re.S)
                    for match in matches:
                        text = re.sub(r'<[^>]+>', '', match.group(1)).strip()
                        text = re.sub(r'\s+', ' ', text)
                        if text and 2 < len(text) < 100 and not any(
                            skip in text.lower() for skip in ['function', 'script', 'style', '{', '}']
                        ):
                            if self.debug:
                                print(f"[parse_category] Знайдено regex: '{text}'")
                            return text
                except Exception as e:
                    if self.debug:
                        print(f"[parse_category] Помилка pattern: {e}")
                    continue

            # Резервный вызов API
            category_name = self.get_category_from_api(category_id)
            if category_name:
                if self.debug:
                    print(f"[parse_category] Знайдено через API: '{category_name}'")
                return category_name

            if self.debug:
                print(f"[parse_category] Категорію з ID {category_id} не знайдено")
            return None
        except Exception as e:
            if self.debug:
                print(f"[parse_category] Загальна помилка: {e}")
            return None

    def get_product_meta(self, product_url, add_data, product_id):
        """Получение метаданных товара"""
        title = None
        category_id = None
        original_url = product_url

        if add_data:
            goods_items = add_data.get('purchases', {}).get('goods', [])
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
                resp = self.scraper.get(original_url, headers=self.base_headers, timeout=15)
                resp.raise_for_status()
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
        if category_id:
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


EXCEL_FILENAME = "rozetka_stock_history.xlsx"
EXCEL_FIELDS = ["name", "url", "category", "last_checked", "max_stock"]


def load_existing_excel(path: str):
    if not os.path.exists(path):
        return pd.DataFrame(columns=EXCEL_FIELDS)
    try:
        df = pd.read_excel(path, engine='openpyxl')
        for col in EXCEL_FIELDS:
            if col not in df.columns:
                df[col] = ''
        return df
    except Exception as e:
        print(f"[ПОПЕРЕДЖЕННЯ] Не вдалося завантажити існуючий Excel файл: {e}")
        print("Створюємо новий файл...")
        return pd.DataFrame(columns=EXCEL_FIELDS)


def save_excel_with_formatting(path: str, df):
    if df.empty:
        print("[ПРЕДУПРЕЖДЕНИЕ] DataFrame пустой, создаем файл только с заголовками")
        df = pd.DataFrame(columns=EXCEL_FIELDS)
    
    products_history = {}
    all_dates = set()
    
    for _, row in df.iterrows():
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
    
    from openpyxl import Workbook
    wb = Workbook()
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
        col_letter = openpyxl.utils.get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = 12

    for row in ws.iter_rows():
        ws.row_dimensions[row[0].row].height = 25

    ws.freeze_panes = 'A2'
    max_row = len(products_history) + 1
    max_col = len(headers)
    ws.auto_filter.ref = f"A1:{openpyxl.utils.get_column_letter(max_col)}{max_row}"

    try:
        wb.save(path)
    except Exception as e:
        print(f"[ОШИБКА] Не удалось сохранить Excel файл: {e}")
        raise


def upsert_rows(df, new_items):
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
    new_df = pd.DataFrame(new_rows, columns=EXCEL_FIELDS)
    if df.empty:
        df = pd.DataFrame(columns=EXCEL_FIELDS)
    for col in EXCEL_FIELDS:
        if col not in df.columns:
            df[col] = ''
    if not new_df.empty and 'url' in new_df.columns and 'url' in df.columns:
        df = df[~df['url'].isin(new_df['url'])]
        df = pd.concat([df, new_df], ignore_index=True)
    elif not new_df.empty:
        df = new_df
    return df


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
    return urls


def parse_cli():
    p = argparse.ArgumentParser(description="Rozetka stock checker -> Excel таблиця")
    p.add_argument('urls', nargs='*', help='URL товарів')
    p.add_argument('-f', '--file', help='Файл зі списком URL (по 1 в рядку)')
    p.add_argument('--interactive', action='store_true', help='Інтерактивний режим для вводу URL')
    p.add_argument('--debug', action='store_true', help='Дебаг вивід')
    p.add_argument('--delay', type=float, default=0.7, help='Затримка між запитами під час бінарного пошуку')
    return p.parse_args()


def main():
    print("🚀 Запуск Rozetka Stock Checker...")
    args = parse_cli()
    urls = list(args.urls)
    if args.file:
        print(f"📄 Завантажуємо URL з файлу: {args.file}")
        urls.extend(read_urls_from_file(args.file))
    if not urls:
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


if __name__ == '__main__':
    main()
