import os
import sqlite3
from dotenv import load_dotenv

def find_last_successive_offset_in_db(db_filename):
    connection_obj = None
    load_dotenv()
    db_fn = os.path.join(".", os.getenv("PROZORRO_DB_FOLDER"), db_filename)
    error_code = 0
    error_text = "Successful operation"

    try:
        connection_obj = sqlite3.connect(db_fn)
        cursor_obj = connection_obj.cursor()

        statement = """SELECT last_offset 
                    FROM Insertion_log 
                    WHERE error_code = 0 
                    ORDER BY insertion_ID DESC LIMIT 1;"""
        cursor_obj.execute(statement)

        fetch_result = cursor_obj.fetchone()
        if len(fetch_result)==0:
            last_offset=0.0
        else:
            last_offset=fetch_result[0]

    except sqlite3.Error as e:
        print(f"SQLite error {e.args[0]}")
        telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                             err_module="Prozorro Local DB",
                                             err_message="Database last  " + db_fn + "." + e.args[0])
        error_code = -30

    finally:
        if connection_obj:
            connection_obj.close()

    if error_code==0:
        result=last_offset
    else:
        result=error_code

    return result
