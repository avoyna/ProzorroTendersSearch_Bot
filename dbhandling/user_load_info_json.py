import json
import os
import sqlite3
from dotenv import load_dotenv
from tender_telegram_bot import telegram_channel_scripting

def save_user_last_db_load_info_to_json_file(db_connection, db_cursor, username="user1"):
    error_code = 0
    load_dotenv()
    json_user_data = {}
    new_data={}
    json_fn = os.path.join(".",os.getenv("PROZORRO_DB_FOLDER"),os.getenv("prozorro_username_json_filename"))
    if os.path.exists(json_fn):
        with open(json_fn, 'r') as json_file:
            json_user_data = json.load(json_file)
            json_file.close()

    try:
        statement = """SELECT internal_ID, id, dateModified 
                    FROM Tender_List 
                    ORDER BY internal_ID DESC LIMIT 1;"""

        db_cursor.execute(statement)
        internal_ID, id, dateModified = db_cursor.fetchone()

        new_data[username] = {"internal_ID": internal_ID, "id": id, "dateModified": dateModified}
        json_user_data.update(new_data)

        with open(json_fn, 'w') as json_file:
            json.dump(json_user_data, json_file, indent=3)
            json_file.close()

    except sqlite3.Error as e:
        print(f"SQLite error {e.args[0]}")
        telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                      err_module="Prozorro Local DB",
                                                      err_message="Database data selection error." +
                                                                  str(e.args[0]))
        error_code = -83

    return error_code


def get_user_last_db_load_info_from_json_file(username="user1"):
    load_dotenv()
    json_user_data = {}
    json_fn = os.path.join(".",os.getenv("PROZORRO_DB_FOLDER"),os.getenv("prozorro_username_json_filename"))
    if os.path.exists(json_fn):
        with open(json_fn, 'r') as json_file:
            json_user_data = json.load(json_file)
            json_file.close()

    return json_user_data

