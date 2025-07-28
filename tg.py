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
    _HAVE_BS4 = False

class RozetkaStockChecker:
    def __init__(self, debug=False, delay=2):
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False,
                'desktop': True
            },
            delay=10,
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
        """Очищаємо стан сесії перед перевіркою нового товару"""
        self.csrf_token = None
        self.purchase_id = None
        self.scraper = cloudscraper.create_scraper()

    def get_csrf_token(self):
        try:
            headers = self.base_headers.copy()
            headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Dest': 'document',
                'Upgrade-Insecure-Requests': '1'
            })
            resp = self.scraper.get('https://rozetka.com.ua/', headers=headers, timeout=10)
            resp.raise_for_status()

            cookies = self.scraper.cookies.get_dict()
            if self.debug:
                print("[ДЕБАГ] Все куки:", cookies)

            possible_csrf_names = ['_uss-csrf', 'csrf-token', 'X-CSRF-TOKEN', 'csrf_token', '_token']
            for csrf_name in possible_csrf_names:
                if csrf_name in cookies:
                    self.csrf_token = cookies[csrf_name]
                    if self.debug:
                        print(f"[ДЕБАГ] Найден CSRF токен '{csrf_name}': {self.csrf_token}")
                    return True

            html = resp.text
            csrf_patterns = [
                r'name="csrf-token"\s+content="([^"]+)"',
                r'"csrf_token"\s*:\s*"([^"]+)"',
                r'_uss-csrf["\']?\s*[:=]\s*["\']([^"\']+)',
                r'csrfToken["\']?\s*[:=]\s*["\']([^"\']+)',
                r'meta\[name=["\']?_?csrf[-_]?token["\']?\]\s*content=["\']([^"\']+)["\']'
            ]
            for pattern in csrf_patterns:
                match = re.search(pattern, html, re.I)
                if match:
                    self.csrf_token = match.group(1)
                    if self.debug:
                        print(f"[ДЕБАГ] CSRF токен найден в HTML: {self.csrf_token}")
                    return True

            test_url = 'https://uss.rozetka.com.ua/session/cart-se/clear?country=UA&lang=ua'
            test_resp = self.scraper.post(test_url, json={}, headers=self.base_headers, timeout=10)
            cookies = self.scraper.cookies.get_dict()
            for csrf_name in possible_csrf_names:
                if csrf_name in cookies:
                    self.csrf_token = cookies[csrf_name]
                    if self.debug:
                        print(f"[ДЕБАГ] CSRF токен получен после тестового запроса: {self.csrf_token}")
                    return True

            if self.debug:
                print("[ДЕБАГ] CSRF токен не найден")
            return False

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
        """Очищаємо корзину перед додаванням нового товару"""
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
        """Парсинг категории ТОЛЬКО из HTML без API вызовов"""
        try:
            # Добавляем больше заголовков для сервера
            headers = self.base_headers.copy()
            headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin'
            })
            
            resp = self.scraper.get(product_url, headers=headers, timeout=20)
            resp.raise_for_status()
            html = resp.text

            # Сохраняем HTML для отладки (особенно важно на сервере)
            if self.debug:
                debug_file = f"debug_server_{category_id}.html"
                try:
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"[parse_category] HTML сохранен в {debug_file}")
                    print(f"[parse_category] Размер HTML: {len(html)} символов")
                except Exception as e:
                    print(f"[parse_category] Не удалось сохранить HTML: {e}")

            if self.debug:
                print(f"[parse_category] Ищем категорию ID: {category_id} для URL: {product_url}")
                print(f"[parse_category] Статус ответа: {resp.status_code}")

            # Проверяем, есть ли BeautifulSoup
            if _HAVE_BS4:
                try:
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # ПРИОРИТЕТНЫЕ селекторы (в порядке важности)
                    priority_selectors = [
                        # Специфичный селектор для 6-го элемента breadcrumbs
                        'rz-breadcrumbs div:nth-child(6) a',
                        'rz-breadcrumbs > div:nth-child(6) > a',
                        '.rz-breadcrumbs div:nth-child(6) a',
                        
                        # Селекторы для rzrelnofollow и black-link
                        'a[rzrelnofollow].black-link',
                        'a.black-link[rzrelnofollow]',
                        'a[rzrelnofollow][class*="black-link"]',
                        
                        # Селекторы по category_id в href
                        f'a[href*="/c{category_id}/"]',
                        f'a[href*="/ua/c{category_id}/"]',
                        f'a[href*="c{category_id}"]',
                        
                        # Дополнительные breadcrumb селекторы
                        'nav[aria-label="breadcrumb"] a',
                        '.breadcrumb a',
                        '.breadcrumbs a',
                        'ol.breadcrumb a',
                        'ul.breadcrumb a'
                    ]

                    for selector in priority_selectors:
                        try:
                            elements = soup.select(selector)
                            if self.debug:
                                print(f"[parse_category] Селектор '{selector}' нашел {len(elements)} элементов")
                            
                            for element in elements:
                                # Проверяем href на соответствие category_id
                                href = element.get('href', '')
                                if category_id and f'c{category_id}' not in href:
                                    continue
                                    
                                text = element.get_text(strip=True)
                                
                                # Фильтруем плохой текст
                                if (text and 
                                    2 < len(text) < 100 and 
                                    not any(skip in text.lower() for skip in 
                                        ['>', '<', 'img', 'svg', 'icon', 'span', 'function', 'script']) and
                                    not re.search(r'^[\s\n\r]*$', text) and
                                    text not in ['Головна', 'Главная', 'Home', 'Rozetka']):
                                    
                                    if self.debug:
                                        print(f"[parse_category] ✓ Найдено через '{selector}': '{text}'")
                                    return text
                                elif self.debug and text:
                                    print(f"[parse_category] ✗ Отфильтровано '{selector}': '{text}' (длина: {len(text)})")
                                    
                        except Exception as e:
                            if self.debug:
                                print(f"[parse_category] Ошибка селектора '{selector}': {e}")
                            continue
                            
                except Exception as e:
                    if self.debug:
                        print(f"[parse_category] Ошибка BeautifulSoup: {e}")

            # Усиленный regex поиск (фолбэк)
            regex_patterns = [
                # Более точные паттерны для категорий
                rf'<a[^>]+href="[^"]*/?c{category_id}/[^"]*"[^>]*>\s*([^<]+?)\s*</a>',
                rf'<a[^>]+href="[^"]*c{category_id}[^"]*"[^>]*>\s*([^<]*?)\s*</a>',
                
                # Breadcrumbs паттерны
                rf'breadcrumb[^>]*>[^<]*<[^>]*href[^>]*c{category_id}[^>]*>([^<]+)</a>',
                rf'rz-breadcrumbs[^>]*>[^<]*<[^>]*href[^>]*c{category_id}[^>]*>([^<]+)</a>',
                
                # JSON в HTML (часто встречается)
                rf'"text"\s*:\s*"([^"]+)"[^}}{{]*"href"[^}}{{]*c{category_id}',
                rf'"title"\s*:\s*"([^"]+)"[^}}{{]*"url"[^}}{{]*c{category_id}'
            ]

            for i, pattern in enumerate(regex_patterns):
                try:
                    matches = re.finditer(pattern, html, re.I | re.S)
                    found_matches = list(matches)
                    
                    if self.debug:
                        print(f"[parse_category] Regex паттерн {i+1} нашел {len(found_matches)} совпадений")
                    
                    for match in found_matches:
                        text = re.sub(r'<[^>]+>', '', match.group(1)).strip()
                        text = re.sub(r'\s+', ' ', text)
                        text = re.sub(r'[^\w\s\-\u0400-\u04FF]', '', text).strip()
                        
                        if (text and 
                            2 < len(text) < 100 and 
                            not any(skip in text.lower() for skip in 
                                ['function', 'script', 'style', '{', '}', 'var ', 'const ', 'let ']) and
                            text not in ['Головна', 'Главная', 'Home', 'Rozetka']):
                            
                            if self.debug:
                                print(f"[parse_category] ✓ Найдено regex {i+1}: '{text}'")
                            return text
                        elif self.debug and text:
                            print(f"[parse_category] ✗ Отфильтровано regex {i+1}: '{text}'")
                            
                except Exception as e:
                    if self.debug:
                        print(f"[parse_category] Ошибка regex паттерна {i+1}: {e}")
                    continue

            # Если ничего не найдено, возвращаем общее значение
            if self.debug:
                print(f"[parse_category] ✗ Категория с ID {category_id} НЕ найдена")
                print("[parse_category] Возвращаем 'Невідома категорія'")

            return "Невідома категорія"
            
        except Exception as e:
            if self.debug:
                print(f"[parse_category] КРИТИЧЕСКАЯ ошибка: {e}")
                import traceback
                traceback.print_exc()
            return "Помилка отримання категорії"

    def get_product_meta(self, product_url, add_data, product_id):
        """ИСПРАВЛЕННАЯ функция получения метаданных товара с дополнительной отладкой"""
        title = None
        category_id = None
        original_url = product_url
        
        if self.debug:
            print(f"[get_product_meta] Обрабатываем товар ID: {product_id}")
            print(f"[get_product_meta] URL: {product_url}")
            print(f"[get_product_meta] Есть add_data: {add_data is not None}")
        
        # Сначала пытаемся получить данные из API ответа корзины
        if add_data:
            goods_items = add_data.get('purchases', {}).get('goods')
            if goods_items:
                for item in goods_items:
                    goods = item.get('goods', {})
                    if goods.get('id') == product_id:
                        title = goods.get('title') or goods.get('name') or None
                        category_id = goods.get('category_id') or None
                        
                        # Обновляем URL если есть лучший вариант
                        api_url = goods.get('href') or goods.get('url')
                        if api_url:
                            product_url = api_url
                        
                        if self.debug:
                            print(f"[get_product_meta] Из API корзины: title='{title}', category_id={category_id}")
                        break
        
        # Если не удалось получить из API, пробуем парсинг HTML
        if not title or not category_id:
            try:
                if self.debug:
                    print(f"[get_product_meta] Парсим HTML для получения недостающих данных")
                
                headers = self.base_headers.copy()
                headers.update({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Cache-Control': 'no-cache'
                })
                
                resp = self.scraper.get(original_url, headers=headers, timeout=20)
                html = resp.text
                
                if self.debug:
                    print(f"[get_product_meta] HTML получен, размер: {len(html)} символов")
                
                if not title and _HAVE_BS4:
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Селекторы для названия товара
                    title_selectors = [
                        'h1.product__title',
                        'h1[data-testid="product-title"]',
                        '.product-title h1',
                        'h1.rz-product-title',
                        'h1.goods-title',
                        'h1'
                    ]
                    
                    for selector in title_selectors:
                        try:
                            element = soup.select_one(selector)
                            if element:
                                title = element.get_text(strip=True)
                                if title and len(title) > 3:
                                    if self.debug:
                                        print(f"[get_product_meta] Название найдено через '{selector}': '{title[:50]}...'")
                                    break
                        except Exception as e:
                            if self.debug:
                                print(f"[get_product_meta] Ошибка селектора названия '{selector}': {e}")
                
                # Если не нашли category_id в API, ищем в URL
                if not category_id:
                    # Ищем в текущем URL
                    patterns = [
                        r'/c(\d+)/',
                        r'category[_-]?id[=:](\d+)',
                        r'cat[_-]?id[=:](\d+)'
                    ]
                    
                    for pattern in patterns:
                        for url_to_check in [product_url, original_url]:
                            match = re.search(pattern, url_to_check)
                            if match:
                                category_id = int(match.group(1))
                                if self.debug:
                                    print(f"[get_product_meta] category_id найден в URL: {category_id}")
                                break
                        if category_id:
                            break
                    
                    # Если не найден в URL, ищем в HTML
                    if not category_id:
                        html_patterns = [
                            r'"category[_-]?id"\s*:\s*(\d+)',
                            r'"categoryId"\s*:\s*(\d+)',
                            r'data-category[_-]?id\s*=\s*["\'](\d+)["\']'
                        ]
                        
                        for pattern in html_patterns:
                            match = re.search(pattern, html, re.I)
                            if match:
                                category_id = int(match.group(1))
                                if self.debug:
                                    print(f"[get_product_meta] category_id найден в HTML: {category_id}")
                                break
                    
            except Exception as e:
                if self.debug:
                    print(f"[get_product_meta] Ошибка парсинга HTML: {e}")
        
        # Получаем название категории ТОЛЬКО через HTML
        category_name = None
        if category_id is not None:
            if self.debug:
                print(f"[get_product_meta] Получаем название категории для ID: {category_id}")
            
            category_name = self.parse_category_from_html(product_url, category_id)
            
            if not category_name or category_name in ['Невідома категорія', 'Помилка отримання категорії']:
                if self.debug:
                    print(f"[get_product_meta] Пробуем альтернативный URL для получения категории")
                
                # Пробуем другие URL если есть
                if product_url != original_url:
                    category_name = self.parse_category_from_html(original_url, category_id)
        
        if self.debug:
            print(f"[get_product_meta] ИТОГОВЫЙ результат:")
            print(f"  - title: '{title}'")
            print(f"  - category_name: '{category_name}'") 
            print(f"  - category_id: {category_id}")
            
        return title, category_name


    def get_category_from_api(self, category_id):
        """Спроба отримати категорію через API Rozetka"""
        try:
            api_url = f"https://common-api.rozetka.com.ua/v2/fat-menu/full?country=UA&lang=ua"
            resp = self.scraper.get(api_url, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                
                def find_category_recursive(items, target_id):
                    for item in items:
                        if item.get('id') == target_id:
                            return item.get('title', item.get('name', ''))
                        
                        children = item.get('children', [])
                        if children:
                            result = find_category_recursive(children, target_id)
                            if result:
                                return result
                    return None
                
                result = find_category_recursive(data.get('data', []), category_id)
                if result:
                    return result
                    
        except Exception as e:
            if self.debug:
                print(f"[get_category_from_api] Помилка: {e}")
        
        return None

    def check_product(self, product_url):
        """Основная функция проверки товара с улучшенной обработкой ошибок"""
        # Сбрасываем состояние сессии
        self.reset_session_state()
        
        product_id = self.extract_product_id(product_url)
        if not product_id:
            error_msg = "Не удалось извлечь ID товара из URL"
            if self.debug:
                print(f"[check_product] ОШИБКА: {error_msg}")
            return {"error": error_msg, "url": product_url}

        if self.debug:
            print(f"[check_product] ===== НАЧАЛО ПРОВЕРКИ ТОВАРА =====")
            print(f"[check_product] ID товара: {product_id}")
            print(f"[check_product] URL: {product_url}")
        
        print(f"=== Проверяем товар ID {product_id}: {product_url}")
        
        # Получаем максимальное количество товара
        max_stock, add_data = self.binary_search_max_stock(product_id)
        if max_stock is None:
            error_msg = "Не удалось определить количество товара"
            if self.debug:
                print(f"[check_product] ОШИБКА: {error_msg}")
            return {"error": error_msg, "url": product_url, "product_id": product_id}

        if self.debug:
            print(f"[check_product] Максимальное количество: {max_stock}")
            print(f"[check_product] Получаем метаданные товара...")

        # Получаем метаданные товара
        try:
            title, category_name = self.get_product_meta(product_url, add_data, product_id)
        except Exception as e:
            if self.debug:
                print(f"[check_product] ОШИБКА получения метаданных: {e}")
                import traceback
                traceback.print_exc()
            title, category_name = "Ошибка получения названия", "Ошибка получения категории"
        
        # Формируем результат
        result = {
            "product_id": product_id,
            "url": product_url,
            "title": title or 'Без названия',
            "category": category_name or 'Без категории',
            "max_stock": max_stock,
        }
        
        if self.debug:
            print(f"[check_product] ===== РЕЗУЛЬТАТ =====")
            for key, value in result.items():
                print(f"[check_product] {key}: {value}")
            print(f"[check_product] ===== КОНЕЦ ПРОВЕРКИ =====")
        
        print(f"✅ Результат для товара {product_id}: {max_stock} шт. | {title or 'Без названия'} | {category_name or 'Без категории'}")
        return result

EXCEL_FILENAME = "rozetka_stock_history.xlsx"
EXCEL_FIELDS = ["name", "url", "category", "last_checked", "max_stock"]

def load_existing_excel(path: str):
    """Загружает данные из Excel в список словарей"""
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
    """Сохраняет список словарей в Excel с форматированием"""
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

def upsert_rows(existing_data, new_items):
    """Обновляет список данных новыми элементами"""
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
            print("⏱️  Пауза між запитами...")
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
