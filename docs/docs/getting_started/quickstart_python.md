# Quickstart Guide (Python)

We will download/install `pyverdict` (a Python interface to VerdictDB) and issues a simple query.
In this Quickstart Guide, we will use MySQL for VerdictDB's backend database.


## Install

`pyverdict` is distributed with PyPI. Use the following command for installation. 

```
pip install pyverdict
```
or
```
pip install pyverdict --upgrade
```

!!! warn "Note: Prerequisites"
    `pyverdict` requires [miniconda](https://conda.io/docs/user-guide/install/index.html) for Python 3.7,
    which can be installed for local users (i.e., without sudo access).


## Connect

```python
import pyverdict
verdict = pyverdict.mysql_context('localhost', 'root', '')
verdict.sql('show schemas')     # this returns pandas DataFrame containing schema names
```

