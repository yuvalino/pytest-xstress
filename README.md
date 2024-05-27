# pytest-xstress

custom xdist scheduler that runs all tests infinitely.

## Usage

```sh
$ pytest -nauto --dist loadgroup --xstress
```

xstress will switch the `loadgroup` scheduler to its own `loadgroupstress` scheduler that will run tests forever with its scheduling taking the group constraint into account. Such scheduling algorithm will try to be as fair as possible, which means running tests in an evenly distributed manner.

**NOTE:** The `-n` switch is important! Running xdist with only one process effectively runs without test distribution at all.

Example result:
```sh
(pytest-xstress-py3.12) ...@macbookpro pytest-xstress % python3 -m pytest test.py -n2 --dist loadgroup --xstress -v
============================= test session starts ==============================
platform darwin -- Python 3.12.2, pytest-8.2.1, pluggy-1.5.0 -- /Users/.../Library/Caches/pypoetry/virtualenvs/pytest-xstress-M8wNAFyb-py3.12/bin/python3
cachedir: .pytest_cache
rootdir: /Users/.../Development/pytest-xstress
configfile: pyproject.toml
plugins: publish-1.0.0, xstress-1.0.0, xdist-3.6.1
created: 2/2 workers

test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 
test.py::_test_single@a 
[gw0] [100%] PASSED test.py::_test_single@a 

...
```
