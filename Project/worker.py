#!/usr/bin/env python
import time
import os
import subprocess



if __name__ == "__main__" :
    active = None
    if os.getenv("TYPE") is None :
        # slave
        print("Slave is active", flush = True)
        active = subprocess.Popen(["sh","shS.sh"])
        while os.getenv("TYPE") is None :
            print("slave env same : ", flush=True)
            wait = 1
            time.sleep(1)
        active.terminate()
        print("Slave becomes master : ", flush=True)
        active = subprocess.Popen(["sh","shM.sh"])
    else :
            active = subprocess.Popen(["sh" ,"shM.sh"])
            while os.environ.get("TYPE") is not None :
                wait = 1
                time.sleep(10)
            #active.terminate()
            #active = subprocess.Popen(["sh","shS.sh"])

    while True :
        wait = 1
