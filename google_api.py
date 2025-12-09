# indexer.py

import datetime
import json
import os
import time

import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv


# ==========================
# Загрузка настроек из .env
# ==========================

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/indexing"]

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "")

JSON_KEYS_DIR = os.getenv("JSON_KEYS_DIR", "json_keys")
URLS_FILE = os.getenv("URLS_FILE", "urls.csv")
RESULT_STORAGE = os.getenv("RESULT_STORAGE", "txt_file")  # txt_file / database
BAD_URLS_LOG = os.getenv("BAD_URLS_LOG", "bad_urls.txt")

REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))
DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() in ("1", "true", "yes")


# ==========================
# Вспомогательные функции
# ==========================

def write_result(storage_type: str, url: str, date_value: datetime.date) -> None:
    """Сохранить успешно отправленный URL."""
    # Всегда пишем в файл, storage_type можно игнорировать
    with open("result.txt", "a", encoding="utf-8") as f:
        f.write(f"{url};{date_value}\n")


def log_bad_url(url: str, info: str) -> None:
    """Лог проблемных URL, которые не надо слать повторно."""
    with open(BAD_URLS_LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now().isoformat()} | {url} | {info}\n")


def index_url(url: str, http: httplib2.Http) -> dict:
    """
    Отправка одного URL в Indexing API.

    Возвращает dict:
      ok: True/False
      fatal_for_key: True/False  (исчерпана квота по ключу)
      code, status: информация об ошибке (если есть)
      data: ответ API при успехе
    """
    endpoint = "https://indexing.googleapis.com/v3/urlNotifications:publish"
    payload = {"url": url.strip(), "type": "URL_UPDATED"}

    if DRY_RUN:
        print(f"[DRY-RUN] Отправили бы: {url}")
        return {
            "ok": True,
            "fatal_for_key": False,
            "code": None,
            "status": None,
            "data": None,
        }

    response, content = http.request(
        endpoint,
        method="POST",
        body=json.dumps(payload),
    )

    try:
        data = json.loads(content.decode())
    except json.JSONDecodeError:
        print(f"Некорректный JSON от API для URL: {url}")
        return {
            "ok": False,
            "fatal_for_key": False,
            "code": None,
            "status": "INVALID_JSON",
            "data": None,
        }

    if "error" in data:
        err = data["error"]
        code = err.get("code")
        status = err.get("status")
        message = err.get("message")

        print(f"Error({code} - {status}): {message}")

        if code == 429 or status == "RESOURCE_EXHAUSTED":
            # Квота кончилась для данного ключа
            return {
                "ok": False,
                "fatal_for_key": True,
                "code": code,
                "status": status,
                "data": None,
            }

        # Ошибка по URL / правам и т.п.
        return {
            "ok": False,
            "fatal_for_key": False,
            "code": code,
            "status": status,
            "data": None,
        }

    meta = data.get("urlNotificationMetadata", {})
    print(f"URL успешно отправлен на переиндексацию: {meta.get('url', url)}")

    latest = meta.get("latestUpdate")
    if latest:
        print("Последнее обновление URL:")
        print(f"- URL: {latest.get('url', 'N/A')}")
        print(f"- Тип: {latest.get('type', 'N/A')}")
        print(f"- Время уведомления: {latest.get('notifyTime', 'N/A')}")

    return {
        "ok": True,
        "fatal_for_key": False,
        "code": None,
        "status": None,
        "data": data,
    }


def load_urls(path: str) -> list[str]:
    """Загрузить очередь URL из файла."""
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def save_urls(path: str, urls: list[str]) -> None:
    """Сохранить очередь URL в файл."""
    with open(path, "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")


def process_urls_for_key(
    json_key_path: str,
    urls_path: str = URLS_FILE,
    storage_type: str = RESULT_STORAGE,
) -> int:
    """
    Обрабатывает очередь URL для одного сервисного аккаунта.
    Возвращает количество успешно отправленных URL.
    """
    print(f"\n=== Работаем с ключом: {json_key_path} ===")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        json_key_path,
        scopes=SCOPES,
    )
    http = credentials.authorize(httplib2.Http())

    urls = load_urls(urls_path)
    if not urls:
        print("Файл с URL пуст или не найден. Делать нечего.")
        return 0

    remaining_urls: list[str] = []
    processed_count = 0
    quota_exhausted = False

    for url in urls:
        if quota_exhausted:
            remaining_urls.append(url)
            continue

        print(f"\nОбработка URL: {url}")
        result = index_url(url, http)
        time.sleep(REQUEST_DELAY)

        if result["ok"]:
            write_result(storage_type, url, datetime.date.today())
            processed_count += 1
            continue

        if result.get("fatal_for_key"):
            print(f"Квота исчерпана для ключа {json_key_path}")
            quota_exhausted = True
            remaining_urls.append(url)
            continue

        print(f"Проблемный URL, пропускаем: {url}")
        info = f"code={result.get('code')}, status={result.get('status')}"
        log_bad_url(url, info)
        # В очередь не возвращаем

    save_urls(urls_path, remaining_urls)

    print(
        f"Ключ {json_key_path}: успешно отправлено {processed_count}, "
        f"осталось в очереди {len(remaining_urls)}"
    )
    return processed_count


# ==========================
# MAIN
# ==========================

def main() -> None:
    total_processed = 0

    if not os.path.isdir(JSON_KEYS_DIR):
        print(f"Папка с ключами не найдена: {JSON_KEYS_DIR}")
        return

    json_files: list[str] = []
    for root, dirs, files in os.walk(JSON_KEYS_DIR):
        for filename in files:
            if filename.lower().endswith(".json"):
                json_files.append(os.path.join(root, filename))

    if not json_files:
        print(f"В папке {JSON_KEYS_DIR} нет JSON-файлов с ключами.")
        return

    json_files.sort()

    for json_key_path in json_files:
        processed = process_urls_for_key(json_key_path)
        total_processed += processed

        if not load_urls(URLS_FILE):
            print("\nОчередь URL опустела. Работа завершена.")
            break

    print(f"\nВсего отправлено на индексацию: {total_processed} шт.")


if __name__ == "__main__":
    # Напоминание: использование множества сервисных аккаунтов
    # для обхода лимитов Google может нарушать условия API.
    main()
