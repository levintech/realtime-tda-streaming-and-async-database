import psutil
import sys
import time
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
        call(["python3", "tda-streaming.py"])
        print('run the script again')