import time
import sys
import subprocess
from py4j.java_gateway import JavaGateway
import pyverdict

class JVM:
    def __init__(self):
        subprocess.Popen(['java', '-cp', pyverdict.JAR_PATH, 'org.verdictdb.VerdictGateway'])
        time.sleep(1)
        self.gateway = JavaGateway()

    def connect(self):
        return self.gateway

    def stop(self):
        self.gateway.shutdown()
