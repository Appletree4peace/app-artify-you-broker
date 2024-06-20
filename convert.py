import sqlite3
from module_python_l10n_logger.logger_config import setup_logger
from module_pushover.pushover import Pushover
import os
import requests

logger = setup_logger(__name__)
comfy_api_server_baseurl = os.getenv('COMFY_API_SERVER_BASEURL')
pushover = Pushover(os.getenv('PUSHOVER_APP_TOKEN'), os.getenv('PUSHOVER_USER_KEY'))

def convert(record):
    try:
        id = record["id"]
        art_styles = record["art_styles"].split(",")
        image_path = record["image_path"]
        upload_langcode = record["upload_langcode"]
        # loop all art_styles and do convertion
        converted_files = []
        for art_style in art_styles:
            converted_path_base = os.path.join('outputs', f"{id}_{art_style}")
            if art_style in ["sitting", "like_for_like", "casual_sit", "standing_singer"]:
                converted_file = art_convert('cute_3d', art_style, image_path, converted_path_base)
            elif art_style in ["film_noir", "neon", "snow", "line_art"]:
                converted_file = art_convert('instantid', art_style, image_path, converted_path_base)
            else:
                raise Exception(f"Unsupported art style: {art_style}")
            
            if converted_file:
                converted_files.append(converted_file)

    except Exception as e:
        logger.error(f"Error opening image: {e}")
        return False
    return converted_files  # Placeholder return value

def art_convert(workflow, art_style, image_path, converted_path_base):
     ## convert image
     # first upload
     logger.info(f"Uploading portrait to comfy api /upload: {converted_path_base} ...")
     response = requests.post(comfy_api_server_baseurl + "/upload", files={'file': open(image_path, 'rb')})
     if response.status_code != 200:
         raise Exception(f"Error uploading image to comfyui api server: {response.text}")
     else:
         response_file_name = response.text

     comfy_api_workflow = workflow
     comfy_api_preset_code = art_style
     comfy_api_endpoint_url = f"{comfy_api_server_baseurl}/wf/{comfy_api_workflow}/submit"
     
     logger.info(f"Calling comfy api workflow submit: {converted_path_base} ...")
     data = {
         "pre_set": comfy_api_preset_code,
         "face": response_file_name
     }

     response = requests.post(comfy_api_endpoint_url, data=data)
     if response.status_code != 200:
        raise Exception(f"Error calling comfy ui api server /submit: {response.json().get('message')}")
     else:
        with open(f"{converted_path_base}.png", 'wb') as image_file:
            image_file.write(response.content)
        logger.info(f"Image successfully saved to {converted_path_base}.png")
        return f"{converted_path_base}.png"
   

def update_converted_status(db_connection, record_id, converted_path = None):
    cursor = db_connection.cursor()
    if converted_path is None:
        cursor.execute("UPDATE kv_pairs SET converted = 1 WHERE id = ?", (record_id,))
    else:
        cursor.execute("UPDATE kv_pairs SET converted = 1, converted_path = ? WHERE id = ?", (converted_path, record_id))
    db_connection.commit()

def process_records():
    db_connection = sqlite3.connect("kv_store.db")
    db_connection.row_factory = sqlite3.Row
    cursor = db_connection.cursor()
    
    cursor.execute("SELECT id, art_styles, image_path, upload_langcode FROM kv_pairs WHERE converted = 0")
    records = cursor.fetchall()
    
    for record in records:
        record_id = record["id"]
        logger.info(f"Processing record with ID: {record_id}")
        update_converted_status(db_connection, record_id)
        converted_files = convert(record)
        if len(converted_files):
            update_converted_status(db_connection, record_id, ",".join(converted_files))
        logger.info(f"Successfully processed record with ID: {record_id}")
        pushover.send("Success, convertion", f"Successfully processed all styles with ID: {record_id}")
    
    db_connection.close()

# Call the function to start processing
if __name__ == "__main__":
    try:
        process_records()
    except Exception as e:
        message = f"Error occurred during convert.py: {e}"
        pushover.send("Error", message)
        logger.error(f"Error occurred during convert.py: {e}", exc_info=e)
