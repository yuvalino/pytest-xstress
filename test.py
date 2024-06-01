import contextlib
import logging
import random
import subprocess
import threading
import time
from collections import defaultdict
from typing import List

import pytest
import werkzeug
from flask import Flask, request

# This spams on each test completed elsewise
logging.getLogger("werkzeug").setLevel(logging.ERROR)


@contextlib.contextmanager
def publish_mock_server(fn, port: int):
    rest_app = Flask("PublishMock")
    rest_server = werkzeug.serving.make_server("localhost", port, rest_app)
    rest_thread = threading.Thread(target=rest_server.serve_forever)

    @rest_app.route("/test-update", methods=["POST"])
    def test_update():
        fn(request.json)
        return ""

    rest_thread.start()
    try:
        yield
    finally:
        rest_server.shutdown()
        rest_thread.join()


@contextlib.contextmanager
def publish_mock(cmdline: List[str], fn, port=7777, expect=-9):
    with publish_mock_server(fn, port):
        ncmdline = cmdline + ["--publish", f"http://localhost:{port}/test-update"]
        print(" ".join(ncmdline))
        p = subprocess.Popen(
            ncmdline,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            yield p
        finally:
            if p.poll() is None:
                p.kill()
                p.wait()
                stdout = "" if p.stdout is None else p.stdout.read().decode()
                raise ValueError(f"process has not exited:\n{stdout}")
            if p.poll() != expect:
                stdout = "" if p.stdout is None else p.stdout.read().decode()
                raise ValueError(
                    f"process did not return expected ({p.poll()} != {expect}):\n{stdout}"
                )


@contextlib.contextmanager
def run_tests(fn, mark: str, *args):
    with publish_mock(
        [
            "python3",
            "-m",
            "pytest",
            __file__,
            "-o",
            "python_functions='_test_*'",
            "-m",
            mark,
            "-v",
        ]
        + list(args),
        fn,
    ) as p:
        yield p


def wait_until(condition, interval=0.1, timeout=2, *args):
    start = time.time()
    while not condition(*args):
        if time.time() - start >= timeout:
            return False
        time.sleep(interval)
    return True


@pytest.mark.xdist_group(name="a")
@pytest.mark.single
def _test_single():
    pass


def test_single():
    datas = list()
    lock = threading.Lock()

    def _on_update(x):
        with lock:
            datas.append(x)

    def _predicate():
        with lock:
            return len(datas) > 10

    with run_tests(
        _on_update, "single", "-n2", "--dist", "loadgroup", "--xstress"
    ) as p:
        assert wait_until(_predicate)
        p.kill()
        p.wait()

    worker = datas[0]["xdist_worker"]
    assert worker
    for x in datas:
        assert x["nodeid"].endswith("::_test_single@a")
        assert x["xdist_worker"] == worker


@pytest.mark.xdist_group(name="b")
@pytest.mark.multiple
def _test_multiple1():
    pass


@pytest.mark.xdist_group(name="b")
@pytest.mark.multiple
def _test_multiple2():
    pass


@pytest.mark.xdist_group(name="b")
@pytest.mark.multiple
def _test_multiple3():
    pass


def test_multiple():
    datas_serial = list()
    datas = defaultdict(lambda: [])
    lock = threading.Lock()
    node_ids_endings = [
        "::_test_multiple1@b",
        "::_test_multiple2@b",
        "::_test_multiple3@b",
    ]

    def _on_update(x):
        with lock:
            datas_serial.append(x)
            datas[x["nodeid"]].append(x)

    def _predicate():
        with lock:
            return len(datas) > 0 and len(datas[next(iter(datas.keys()))]) > 10

    with run_tests(
        _on_update, "multiple", "-n2", "--dist", "loadgroup", "--xstress"
    ) as p:
        assert wait_until(_predicate)
        p.kill()
        p.wait()

    assert len(datas) == len(node_ids_endings)
    first_key = next(iter(datas.keys()))
    worker = datas[first_key][0]["xdist_worker"]
    assert worker

    keys_itr = iter(datas.keys())
    for x in node_ids_endings:
        assert next(keys_itr).endswith(x)

    # ensure tests serial aka 1,2,3,1,2,3,1,2,3,1 and not 1,2,3,1,2,3,3,1,2,3
    for idx, test in enumerate(datas_serial):
        assert (test["nodeid"]).endswith(node_ids_endings[idx % len(node_ids_endings)])


@pytest.mark.xdist_group(name="c1")
@pytest.mark.multisingle
def _test_multisingle1():
    pass


@pytest.mark.xdist_group(name="c2")
@pytest.mark.multisingle
def _test_multisingle2():
    pass


def test_multisingle():
    datas_by_worker = defaultdict(lambda: [])

    lock = threading.Lock()

    def _on_update(x):
        with lock:
            datas_by_worker[x["xdist_worker"]].append(x)

    def _predicate():
        with lock:
            return (
                len(datas_by_worker) >= 2
                and len(datas_by_worker["gw0"]) > 10
                and len(datas_by_worker["gw1"]) > 10
            )

    with run_tests(
        _on_update, "multisingle", "-n2", "--dist", "loadgroup", "--xstress"
    ) as p:
        assert wait_until(_predicate)
        p.kill()
        p.wait()

    for x in datas_by_worker["gw0"]:
        assert x["nodeid"].endswith("::_test_multisingle1@c1")
    for x in datas_by_worker["gw1"]:
        assert x["nodeid"].endswith("::_test_multisingle2@c2")


@pytest.mark.xdist_group(name="d1")
@pytest.mark.multimultiple
def _test_multimultiple1a():
    pass


@pytest.mark.xdist_group(name="d1")
@pytest.mark.multimultiple
def _test_multimultiple2a():
    pass


@pytest.mark.xdist_group(name="d1")
@pytest.mark.multimultiple
def _test_multimultiple3a():
    pass


@pytest.mark.xdist_group(name="d2")
@pytest.mark.multimultiple
def _test_multimultiple1b():
    pass


@pytest.mark.xdist_group(name="d2")
@pytest.mark.multimultiple
def _test_multimultiple2b():
    pass


@pytest.mark.xdist_group(name="d2")
@pytest.mark.multimultiple
def _test_multimultiple3b():
    pass


def test_multimultiple():
    datas_by_worker = defaultdict(lambda: [])
    datas = defaultdict(lambda: [])
    lock = threading.Lock()
    node_ids_endings_d1 = [
        "::_test_multimultiple1a@d1",
        "::_test_multimultiple2a@d1",
        "::_test_multimultiple3a@d1",
    ]
    node_ids_endings_d2 = [
        "::_test_multimultiple1b@d2",
        "::_test_multimultiple2b@d2",
        "::_test_multimultiple3b@d2",
    ]

    def _on_update(x):
        with lock:
            datas_by_worker[x["xdist_worker"]].append(x)
            datas[x["nodeid"]].append(x)

    def _predicate():
        with lock:
            return (
                len(datas_by_worker) == 2
                and len(datas_by_worker["gw0"]) > 10
                and len(datas_by_worker["gw1"]) > 10
            )

    with run_tests(
        _on_update, "multimultiple", "-n2", "--dist", "loadgroup", "--xstress"
    ) as p:
        assert wait_until(_predicate)
        p.kill()
        p.wait()

    assert len(datas) == len(node_ids_endings_d1) + len(node_ids_endings_d2)

    for idx, test in enumerate(datas_by_worker["gw0"]):
        assert (test["nodeid"]).endswith(
            node_ids_endings_d1[idx % len(node_ids_endings_d1)]
        )
    for idx, test in enumerate(datas_by_worker["gw1"]):
        assert (test["nodeid"]).endswith(
            node_ids_endings_d2[idx % len(node_ids_endings_d2)]
        )


@pytest.mark.xdist_group(name="e1")
@pytest.mark.verymultimultiple
def _test_verymultimultiple1a():
    time.sleep(random.random() / 5)


@pytest.mark.xdist_group(name="e1")
@pytest.mark.verymultimultiple
def _test_verymultimultiple2a():
    pass


@pytest.mark.xdist_group(name="e2")
@pytest.mark.verymultimultiple
def _test_verymultimultiple1b():
    time.sleep(random.random() / 5)


@pytest.mark.xdist_group(name="e2")
@pytest.mark.verymultimultiple
def _test_verymultimultiple2b():
    pass


@pytest.mark.xdist_group(name="e3")
@pytest.mark.verymultimultiple
def _test_verymultimultiple1c():
    time.sleep(random.random() / 5)


@pytest.mark.xdist_group(name="e3")
@pytest.mark.verymultimultiple
def _test_verymultimultiple2c():
    pass


def test_verymultimultiple():
    datas_by_worker = defaultdict(lambda: [])
    datas_by_nodeid = defaultdict(lambda: 0)

    lock = threading.Lock()

    matching = {
        "::_test_verymultimultiple1a@e1": "::_test_verymultimultiple2a@e1",
        "::_test_verymultimultiple1b@e2": "::_test_verymultimultiple2b@e2",
        "::_test_verymultimultiple1c@e3": "::_test_verymultimultiple2c@e3",
    }

    def _on_update(x):
        with lock:
            datas_by_worker[x["xdist_worker"]].append(x["nodeid"])
            datas_by_nodeid[x["nodeid"]] += 1

    def _predicate():
        with lock:
            return (
                len(datas_by_worker) >= 2
                and len(datas_by_worker["gw0"]) > 100
                and len(datas_by_worker["gw1"]) > 100
            )

    with run_tests(
        _on_update, "verymultimultiple", "-n2", "--dist", "loadgroup", "--xstress"
    ) as p:
        assert wait_until(_predicate, timeout=20)
        p.kill()
        p.wait()

    for gw in ["gw0", "gw1"]:
        prev = None
        for x in datas_by_worker[gw]:
            if prev is None:
                ending = next(y for y in matching.keys() if x.endswith(y))
                prev = ending
            else:
                assert any(x.endswith(y) for y in matching.values())
                assert x.endswith(matching[prev])
                prev = None

    assert len(datas_by_nodeid) == 6
    for count in datas_by_nodeid.values():
        assert (
            count >= 25
        )  # since 200 tests are run, this should always be true mathematically
