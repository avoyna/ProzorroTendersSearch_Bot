import psutil
import os

def is_running(script_name):
    for q in psutil.process_iter():
        if q.name().startswith('python'):
            if len(q.cmdline())>1 and script_name in q.cmdline()[1] and q.pid !=os.getpid():
                print("'{}' process is already running".format(script_name))
                return True

    return False


