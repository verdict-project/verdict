import os 
from .jvm import JVM
from .resultset import ResultSet
from .verdictcontext import VerdictContext

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LIB_DIR = os.path.join(ROOT_DIR, 'lib')
JAR_NAME = 'verdictdb-core-0.5.4-SNAPSHOT-jar-with-dependencies.jar'
JAR_PATH = os.path.join(LIB_DIR, JAR_NAME)
