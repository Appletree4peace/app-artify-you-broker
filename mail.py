import sqlite3
from module_python_l10n_logger.logger_config import setup_logger
from module_pushover.pushover import Pushover
from module_gmail_sender.gmail_sender import GmailSender
import os
import yaml

logger = setup_logger(__name__)
pushover = Pushover(os.getenv('PUSHOVER_APP_TOKEN'), os.getenv('PUSHOVER_USER_KEY'))

def load_config(filename):
    with open(filename, 'r') as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as exc:
            logger.error(f"Error loading YAML file: {filename}", exc)
            return None

def process_records():
    db_connection = sqlite3.connect("kv_store.db")
    db_connection.row_factory = sqlite3.Row
    cursor = db_connection.cursor()
    
    config = load_config("config.yaml")
    config = config['mail_convert_success']
    sender = config["sender"]
    sender_id = "me" # this is fixed

    cursor.execute("SELECT id, email, converted_path, converted, upload_langcode FROM kv_pairs WHERE mailsent = 0 AND converted_path IS NOT NULL AND converted_path != ''")
    records = cursor.fetchall()
    
    for record in records:
        logger.info(f"Mailing record with ID: {record['id']}")
        lang_code = record["upload_langcode"]
        subject = config["subject"][lang_code]
        message_text_html = config["body"][lang_code]
        record_id = record["id"]
        converted_path = record["converted_path"].split(",")

        to = record["email"]
        attachment_paths = converted_path

        try:
            GmailSender('gcp-svc-acc-key-gmail.json', 'admin@artfyyou.com').send(subject, message_text_html, sender, sender_id, to, attachment_paths)
            #GmailSender('gcp-svc-acc-key-gmail.json', 'admin@artfyyou.com').send('test', '<p>hello world</p>', sender, sender_id, to, attachment_paths)
            update_mailsent_status(db_connection, record_id)
            logger.info(f"Successfully mailed record with ID: {record_id}")
            pushover.send("Success, mail", f"Successfully mailed record with ID: {record_id}")
        except Exception as e:
            logger.error(f"Error occurred during send(): {e}", exc_info=e)
            pushover.send("Error, mail", f"Error occurred during send(): {e}")
#
#        logger.info(f"Successfully processed record with ID: {record_id}")
#        pushover.send("Success, convertion", f"Successfully processed all styles with ID: {record_id}")
    
    db_connection.close()

def update_mailsent_status(db_connection, record_id):
    cursor = db_connection.cursor()
    cursor.execute("UPDATE kv_pairs SET mailsent = 1 WHERE id = ?", (record_id,))
    db_connection.commit()

if __name__ == "__main__":
    try:
        process_records()
    except Exception as e:
        logger.error(f"Error occurred during mail.py: {e}", exc_info=e)
        pushover.send("Error, mail", f"Error occurred during mail.py: {e}")