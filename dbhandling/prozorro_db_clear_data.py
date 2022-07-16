import os
import sqlite3
from dotenv import load_dotenv

def delete_old_data(db_filename, min_date_to_keep_in_base):
    connection_obj = None
    error_code = 0
    load_dotenv()
    db_fn = os.path.join(".", os.getenv("PROZORRO_DB_FOLDER"), db_filename)

    try:
        connection_obj = sqlite3.connect(db_fn)
        connection_obj.execute("PRAGMA foreign_keys=1;")
        connection_obj.execute("PRAGMA cache_size=-300000;")
        cursor_obj = connection_obj.cursor()

        statement = """DELETE FROM Tender_list 
                    WHERE DATE(dateModified) < ?;"""
        value = (min_date_to_keep_in_base,)
        cursor_obj.execute(statement, value)
        connection_obj.commit()


    except sqlite3.Error as e:
        print(f"SQLite error {e.args[0]}")
        telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                      err_module="Prozorro Local DB",
                                                      err_message="Database old data deleting error ." +
                                                                  str(e.args[0]))
        error_code = -87

    finally:
        if connection_obj:
            connection_obj.close()

    return error_code