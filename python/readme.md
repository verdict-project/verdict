# Python wrapper for Verdict

This directory should be added to your PYTHONPATH environment variable before
running the pyspark shell.

## Usage

```python
from pyverdict import VerdictSparkContext
vc = VerdictSparkContext(sqlContext)
vc.sql("show databases").show()
```

