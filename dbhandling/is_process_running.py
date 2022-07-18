import psutil
import os
from dotenv import load_dotenv


def is_running(script_name):
    for q in psutil.process_iter():
        if q.name().startswith('python'):
            if len(q.cmdline())>1 and script_name in q.cmdline()[1] and q.pid !=os.getpid():
                print("'{}' process is already running".format(script_name))
                return True

    return False

def is_running_all_software(software_name="prozorro_bot"):
    if software_name=="prozorro_bot":
        load_dotenv()
        script_names=os.getenv("PROZORRO_EXECUTIVE_SCRIPTS")
        if not script_names == "":
            script_name_list = script_names.split(sep=",")
            for script_name in script_name_list:
                if is_running(script_name):
                    return True

    return False
