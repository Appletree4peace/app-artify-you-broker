import os
import requests
import sqlite3
from module_python_l10n_logger.logger_config import setup_logger
import io
from PIL import Image

# Cloudflare API Information
API_BASE_URL = os.getenv('API_BASE_URL')
ACCOUNT_ID   = os.getenv('ACCOUNT_ID')
NAMESPACE_ID = os.getenv('NAMESPACE_ID')
API_EMAIL    = os.getenv('API_EMAIL')
API_KEY      = os.getenv('API_KEY')

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

logger = setup_logger(__name__)

def setup_database():
    conn = sqlite3.connect('kv_store.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS kv_pairs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT, value blob, email TEXT, art_styles TEXT, file_extension TEXT, image_path TEXT, converted_path TEXT, lang_code TEXT, upload_langcode TEXT, converted bool DEFAULT 0, mailsent bool DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (email TEXT PRIMARY KEY, requests INTEGER DEFAULT 0, lang_code TEXT)''')
    conn.commit()
    conn.close()

def list_keys():
    logger.info('Listing keys...')
    url = f"{API_BASE_URL}/accounts/{ACCOUNT_ID}/storage/kv/namespaces/{NAMESPACE_ID}/keys"
    response = requests.get(url, headers=headers)
    print(response)
    keys = response.json().get("result", [])
    return [key["name"] for key in keys]

def get_kv_value(key):
    logger.info(f'Getting value for key: {key}')
    url = f"{API_BASE_URL}/accounts/{ACCOUNT_ID}/storage/kv/namespaces/{NAMESPACE_ID}/values/{key}"
    response = requests.get(url, headers=headers)
    return response.content

def get_kv_metadata(key):
    logger.info(f'Getting metadata for key: {key}')
    url = f"{API_BASE_URL}/accounts/{ACCOUNT_ID}/storage/kv/namespaces/{NAMESPACE_ID}/metadata/{key}"
    response = requests.get(url, headers=headers)
    return response.json()

def delete_kv_pair(key):
    logger.info(f'Deleting key: {key}')
    url = f"{API_BASE_URL}/accounts/{ACCOUNT_ID}/storage/kv/namespaces/{NAMESPACE_ID}/values/{key}"
    requests.delete(url, headers=headers)

def store_kv_pairs_locally():
    setup_database()
    conn = sqlite3.connect('kv_store.db')
    c = conn.cursor()
    
    for key in list_keys():
        logger.info(f'--------------- Storing key: {key} ------------------')
        value = get_kv_value(key)
        metadata = get_kv_metadata(key)
        email = metadata['result']['email']
        art_styles = metadata['result']['art_styles']
        file_extension = metadata['result']['fileExtension']
        lang_code = metadata['result']['lang_code']
        upload_langcode = metadata['result']['upload_langcode']

        c.execute("INSERT OR REPLACE INTO kv_pairs (key, value, email, art_styles, file_extension, upload_langcode) VALUES (?, ?, ?, ?, ?, ?)", (key, value, email, art_styles, file_extension, upload_langcode))
        record_id = c.lastrowid
        c.execute("UPDATE users SET requests = requests + 1 WHERE email = ?", (email,))
        # If no rows were updated, insert a new record
        if c.rowcount == 0:
            c.execute("INSERT INTO users (email, requests, lang_code) VALUES (?, 1, ?)", (email, lang_code))

        # Store portrat to local
        image_stream = io.BytesIO(value)
        image = Image.open(image_stream)
        #format = image.format
        output_portrait_file = os.path.join('uploads', f'{record_id}_portrait.{file_extension}')
        logger.info(f'Saving portrait to: {output_portrait_file}')
        image.save(output_portrait_file)

        # Update database
        c.execute("UPDATE kv_pairs SET image_path = ? WHERE id = ?", (output_portrait_file, record_id))

        logger.info(f'Deleting remote key: {key}')
        delete_kv_pair(key)
    
    conn.commit()
    conn.close()

def delete_kv_pairs():
    for key in list_keys():
        delete_kv_pair(key)

def main():
    try:
        store_kv_pairs_locally()
    except Exception as e:
        logger.error(f"Error occurred during store_kv_pairs_locally(): {e}", exc_info=e)

if __name__ == "__main__":
    main()