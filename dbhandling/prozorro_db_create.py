import sqlite3
import os.path
from dotenv import load_dotenv
from tender_telegram_bot import telegram_channel_scripting
from datetime import datetime

def create_db(db_filename):
    """
    Creates SQLite database file within the folder defined by environment variables
    :param db_filename: database name
    :return: error code
        0 - succeeded
        -1 - wrong DB
        -2 - impossible to connect
    """

    conn = None
    error_code = 0
    existing_file_is_correct_db = True
    load_dotenv()
    db_fn = os.path.join(".",os.getenv("PROZORRO_DB_FOLDER"),db_filename)

    # print(db_fn)
    if os.path.isfile(db_fn):
        if os.path.getsize(db_fn) < 100: # SQLite database file header is 100 bytes
            existing_file_is_correct_db = False

        with open(db_fn, 'rb') as fd:
            header = fd.read(100)
            if header[:16] == 'SQLite format 3\x00':
                existing_file_is_correct_db = False
            fd.close()

    if not existing_file_is_correct_db:
        print("Existing DB file is not correct", db_fn)

        telegram_channel_scripting.raise_tech_message(telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN"),
                           err_module = "Prozorro Local DB",
                           err_message = "Database "+ db_fn + " is corrupted and not correct")

        return -1

    # if not os.path.isfile(db_fn):
    #     existing_file_is__correct_db = False

    try:
        conn = sqlite3.connect(db_fn)
        conn.execute("PRAGMA foreign_keys=1;")
        # print(sqlite3.version)
    except Error as e:
        print(e)
        telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                err_module="Prozorro Local DB",
                                err_message="Error connecting to database " + db_fn + "." +e)
        error_code = -2
    finally:
        if conn:
            conn.close()

    return error_code


def create_prozorro_tables(db_filename):
    connection_obj = None
    load_dotenv()
    db_fn = os.path.join(".",os.getenv("PROZORRO_DB_FOLDER"),db_filename)

    try:
        connection_obj = sqlite3.connect(db_fn)
        connection_obj.execute("PRAGMA foreign_keys=1;")
        cursor_obj = connection_obj.cursor()

        table = """CREATE TABLE IF NOT EXISTS Insertion_log (
                insertion_ID INTEGER PRIMARY KEY,
                insertion_datetime DATETIME,
                inserted_by_user TEXT,
                error_code INTEGER,
                error_text TEXT,
                last_offset REAL
            ); """

        # table = "create table IF NOT EXISTS films  (title text, year text, director text)"
        cursor_obj.execute(table)
        connection_obj.commit()

        table = """CREATE TABLE IF NOT EXISTS Tender_list (
                        internal_ID INTEGER PRIMARY KEY,
                        id TEXT,
                        dateModified DATETIME,
                        insertion_ID INTEGER,
                        FOREIGN KEY (insertion_ID) REFERENCES Insertion_log (insertion_ID) ON DELETE CASCADE
                    ); """

        cursor_obj.execute(table)
        connection_obj.commit()

        table = """CREATE TABLE IF NOT EXISTS procuringEntity (
                        internal_ID INTEGER PRIMARY KEY,
                        name TEXT,
                        name_en TEXT,
                        identifier_scheme TEXT,
                        identifier_id TEXT,
                        identifier_legalName TEXT,
                        identifier_legalName_en TEXT,
                        address_postalCode TEXT,
                        address_countryName TEXT,
                        address_region TEXT,
                        address_locality TEXT,
                        address_streetAddress TEXT,
                        contactPoint_name TEXT,
                        contactPoint_telephone TEXT,
                        contactPoint_email TEXT,
                        contactPoint_url TEXT,
                        kind TEXT
                    ); """
        cursor_obj.execute(table)
        connection_obj.commit()


        table = """CREATE TABLE IF NOT EXISTS Tender (
                        internal_ID INTEGER PRIMARY KEY,
                        Tender_list_internal_ID INTEGER,
                        title TEXT,
                        description TEXT,
                        auctionUrl TEXT,
                        value_amount REAL,
                        value_currency TEXT,
                        value_valueAddedTaxIncluded BOOLEAN,
                        owner TEXT,
                        date DATETIME,
                        dateCreated DATETIME,
                        dateModified DATETIME,
                        status TEXT,
                        submissionMethod TEXT,
                        procurementMethod TEXT,
                        mainProcurementCategory TEXT,
                        procuringEntity_internal_ID INTEGER,
                        enquiryPeriod_startDate DATETIME,
                        enquiryPeriod_endDate DATETIME,
                        enquiryPeriod_clarificationsUntil DATETIME,
                        enquiryPeriod_invalidationDate DATETIME,
                        tenderPeriod_startDate DATETIME,
                        tenderPeriod_endDate DATETIME,
                        complaintPeriod_startDate DATETIME,
                        complaintPeriod_endDate DATETIME,
                        auctionPeriod_startDate DATETIME,
                        auctionPeriod_endDate DATETIME,
                        id TEXT,
                        tenderID TEXT,
                        FOREIGN KEY (Tender_list_internal_ID) REFERENCES Tender_list (internal_ID) ON DELETE CASCADE,
                        FOREIGN KEY (procuringEntity_internal_ID) REFERENCES procuringEntity (internal_ID) ON DELETE CASCADE
                    ); """
        cursor_obj.execute(table)
        connection_obj.commit()

        table = """CREATE TABLE IF NOT EXISTS items (
                        internal_ID INTEGER PRIMARY KEY,
                        id TEXT,
                        description TEXT,
                        classification_scheme TEXT,
                        classification_id TEXT,
                        classification_description TEXT,
                        quantity REAL,
                        unit_name TEXT,
                        unit_code TEXT,
                        deliveryDate_startDate TEXT,
                        deliveryDate_endDate TEXT,
                        deliveryAddress_postalCode TEXT,
                        deliveryAddress_countryName TEXT,
                        deliveryAddress_region TEXT,
                        deliveryAddress_locality TEXT,
                        deliveryAddress_streetAddress TEXT,
                        deliveryLocation_latitude TEXT,
                        deliveryLocation_longitude TEXT,
                        deliveryLocation_elevation TEXT,
                        tender_internal_ID INTEGER,
                        FOREIGN KEY (tender_internal_ID) REFERENCES Tender (internal_ID) ON DELETE CASCADE
                    ); """
        cursor_obj.execute(table)
        connection_obj.commit()

        statement = """INSERT INTO Insertion_log (insertion_datetime, inserted_by_user, error_code, 
                                error_text, last_offset) VALUES (?, ?, ?, ?, ?); """

        currentDateTime = datetime.now()

        values = (currentDateTime, "db_creator", "100", "Creating empty tables", None)

        cursor_obj.execute(statement, values)
        connection_obj.commit()


    except sqlite3.Error as e:
        print(f"SQLite error {e.args[0]}")
        telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                             err_module="Prozorro Local DB",
                                             err_message="Database creation error in " + db_fn + "." +
                                                         str(e.args[0]))
        return -1

    finally:
        if connection_obj:
            connection_obj.close()

    return 0


def drop_data_tables(db_filename):
    connection_obj = None
    error_code = 0
    load_dotenv()
    db_fn = os.path.join(".", os.getenv("PROZORRO_DB_FOLDER"), db_filename)

    try:
        connection_obj = sqlite3.connect(db_fn)
        connection_obj.execute("PRAGMA foreign_keys=0;")
        cursor_obj = connection_obj.cursor()

        table = "DROP TABLE IF EXISTS items;"
        cursor_obj.execute(table)
        connection_obj.commit()

        table = "DROP TABLE IF EXISTS procuringEntity;"
        cursor_obj.execute(table)
        connection_obj.commit()

        table = "DROP TABLE IF EXISTS Tender;"
        cursor_obj.execute(table)
        connection_obj.commit()

        table = "DROP TABLE IF EXISTS Tender_list;"
        cursor_obj.execute(table)
        connection_obj.commit()

        connection_obj.execute("PRAGMA foreign_keys=1;")
        connection_obj.commit()

    except sqlite3.Error as e:
        print(f"SQLite error {e.args[0]}")
        telegram_channel_scripting.raise_tech_message(telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
                                                      err_module="Prozorro Local DB",
                                                      err_message="Database dropping error in " + db_fn + "." +
                                                                  str(e.args[0]))
        error_code = -80

    finally:
        if connection_obj:
            connection_obj.close()

    return error_code