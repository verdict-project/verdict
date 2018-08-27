import time
import sys
import subprocess
import atexit
from py4j.java_gateway import JavaGateway
from pyverdict import JAR_PATH


class Gateway:
    def __init__(self):
        subprocess.Popen(['java', '-cp', JAR_PATH, 'org.verdictdb.VerdictGateway'])
        time.sleep(1) # TODO: check whether the jvm started before connecting to it
        self.gateway = JavaGateway()
        atexit.register(self.close)

    def connect(self):
        return self.gateway

    def close(self):
        self.gateway.shutdown()
