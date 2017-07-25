# Python wrapper for Verdict

This directory should be added to your PYTHONPATH environment variable before
running the pyspark shell.

```bash
export PYTHONPATH=$(pwd):$PYTHONPATH
```

## With pyspark Shell

```bash
pyspark --driver-class-path target/verdict-core-(version)-jar-with-dependencies.jar
```

## Usage

```python
from pyverdict import VerdictHiveContext
vc = VerdictHiveContext(sc)        # sc: SparkContext instance
vc.sql("show databases").show()
```

