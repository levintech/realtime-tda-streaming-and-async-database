import psutil
import sys
import time
from datetime import datetime
import pytz
from subprocess import Popen
from subprocess import call

while True:
    is_running = False
    for process in psutil.process_iter():
        if process.cmdline() == ['python3', 'tda-streaming.py']:
            is_running = True
            time.sleep(1)
            break
    
    if not is_running:
        content = ""
        with open("log.txt", "r") as logger:
            content = logger.read()

        if 'End of day' in content:
            sys.exit()

        current_time = datetime.now(pytz.timezone('US/Eastern'))
        current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

        with open("log.txt", "a") as logger:
            logger.write("{}\n".format(current_time))
            logger.write("Script is started\n")
            logger.write('--------------------------------------------------\n')

        call(["python3", "tda-streaming.py"])
