import time
import os
import subprocess
import atexit
from py4j.java_gateway import JavaGateway


class Gateway:
    def __init__(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        lib_dir = os.path.join(root_dir, 'lib')
        jar_name = 'verdictdb-core-0.5.4-SNAPSHOT-jar-with-dependencies.jar'
        jar_path = os.path.join(lib_dir, jar_name)
        subprocess.Popen(['java', '-cp', jar_path, 'org.verdictdb.VerdictGateway'])
        time.sleep(1) # TODO: check whether the jvm started before connecting to it
        self.gateway = JavaGateway()
        atexit.register(self.close)

    def connect(self, url, usr, pwd):
        return self.gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(url, usr, pwd)

    def close(self):
        self.gateway.shutdown()
