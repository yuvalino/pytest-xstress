from __future__ import annotations

import functools
from itertools import chain

import pytest
from xdist.remote import Producer
from xdist.scheduler.loadgroup import LoadGroupScheduling
from xdist.workermanage import WorkerController

MIN_TESTS_PER_NODE = 2


class LoadGroupStressScheduler(LoadGroupScheduling):
    def __init__(self, config: pytest.Config, log: Producer | None = None) -> None:
        super().__init__(config, log)
        if log is None:
            self.log = Producer("loadgroupstresssched")
        else:
            self.log = log.loadgroupstresssched

        self.assigned_work: dict[WorkerController, dict[str, dict[str, int]]] = {}
        self.collection: list[str] | None = None

    @property
    def tests_finished(self) -> bool:
        return False

    @property
    def has_pending(self) -> bool:
        return True

    def _pending_of(self, workload: dict[str, dict[str, int]]) -> int:
        """Return the number of pending tests in a workload."""
        pending = sum(sum(scope.values()) for scope in workload.values())
        return pending

    def mark_test_complete(
        self, node: WorkerController, item_index: int, duration: float = 0
    ) -> None:
        """Add more tests to node if `pending_tests_in_node<MIN_TESTS_PER_NODE`"""
        nodeid = self.registered_collections[node][item_index]
        scope = self._split_scope(nodeid)

        if self.assigned_work[node][scope][nodeid]:
            self.assigned_work[node][scope][nodeid] -= 1

        # If the current scope has been finished, return it to the workqueue to be re-run
        if sum(self.assigned_work[node][scope].values()) == 0:
            work_unit = self.assigned_work[node].pop(scope)
            for curr_nodeid in work_unit:
                work_unit[curr_nodeid] = False
            self.workqueue[scope] = work_unit

        # If not enough tests in current node, add more scopes if available or repeat tests
        while (
            self._pending_of(self.assigned_work[node]) < MIN_TESTS_PER_NODE
            and self.workqueue
        ):
            self._assign_work_unit(node)
        while self._pending_of(self.assigned_work[node]) < MIN_TESTS_PER_NODE:
            self._reassign_work_units(node)

    def _assign_work_unit(self, node: WorkerController) -> None:
        """Assign a work unit from `self.workqueue` to a node."""
        assert self.workqueue

        # Grab a unit of work
        scope, work_unit = self.workqueue.popitem(last=False)
        for nodeid in work_unit.keys():
            work_unit[nodeid] = int(
                not work_unit[nodeid]
            )  # True -> finished (count=0), False -> unfinished (count=1)

        # Keep track of the assigned work
        assigned_to_node = self.assigned_work.setdefault(node, {})
        assigned_to_node[scope] = work_unit

        # Ask the node to execute the workload
        worker_collection = self.registered_collections[node]
        nodeids_indexes = list(
            chain(
                *[
                    [worker_collection.index(nodeid)] * count
                    for nodeid, count in work_unit.items()
                ]
            )
        )

        node.send_runtest_some(nodeids_indexes)

    def _reassign_work_units(self, node: WorkerController):
        """Ensure `pending_tests_in_node>=MIN_TESTS_PER_NODE` by reassigning more scopes"""
        for curr_scope in self.assigned_work[node]:
            if self._pending_of(self.assigned_work[node]) >= MIN_TESTS_PER_NODE:
                break

            for curr_nodeid in self.assigned_work[node][curr_scope]:
                self.assigned_work[node][curr_scope][curr_nodeid] += 1

            worker_collection = self.registered_collections[node]
            nodeids_indexes = [
                worker_collection.index(curr_nodeid)
                for curr_nodeid, count in self.assigned_work[node][curr_scope].items()
            ]

            node.send_runtest_some(nodeids_indexes)

    def schedule(self) -> None:
        """Initiate distribution of the test collection.

        Initiate scheduling of the items across the nodes.  If this gets called
        again later it behaves the same as calling ``._reschedule()`` on all
        nodes so that newly added nodes will start to be used.

        If ``.collection_is_completed`` is True, this is called by the hook:

        - ``DSession.worker_collectionfinish``.
        """
        assert self.collection_is_completed

        # Initial distribution already happened, reschedule on all nodes
        if self.collection is not None:
            for node in self.nodes:
                self._reschedule(node)
            return

        # Check that all nodes collected the same tests
        if not self._check_nodes_have_same_collection():
            self.log("**Different tests collected, aborting run**")
            return

        # Collections are identical, create the final list of items
        self.collection = list(next(iter(self.registered_collections.values())))
        if not self.collection:
            return

        # Determine chunks of work (scopes)
        unsorted_workqueue: dict[str, dict[str, bool]] = {}
        for nodeid in self.collection:
            scope = self._split_scope(nodeid)
            work_unit = unsorted_workqueue.setdefault(scope, {})
            work_unit[nodeid] = False

        # Insert tests scopes into work queue ordered by number of tests.
        for scope, nodeids in sorted(
            unsorted_workqueue.items(), key=lambda item: -len(item[1])
        ):
            self.workqueue[scope] = nodeids

        # Avoid having more workers than work
        extra_nodes = len(self.nodes) - len(self.workqueue)
        if extra_nodes > 0:
            self.log(f"Shutting down {extra_nodes} nodes")

            for _ in range(extra_nodes):
                unused_node, assigned = self.assigned_work.popitem()

                self.log(f"Shutting down unused node {unused_node}")
                unused_node.shutdown()

        # Assign initial workload
        for node in self.nodes:
            self._assign_work_unit(node)

        # Add more scopes iteratively for nodes with under the minimum amount of tests per node
        while self.workqueue:
            added = False
            for node in self.nodes:
                if not self.workqueue:
                    break
                if self._pending_of(self.assigned_work[node]) < MIN_TESTS_PER_NODE:
                    self._assign_work_unit(node)
                    added = True
            if not added:
                break

        # If we still have nodes without enough tests and we exhausted the workqueue, start re-running existing tests
        for node in self.nodes:
            while self._pending_of(self.assigned_work[node]) < MIN_TESTS_PER_NODE:
                self._reassign_work_units(node)


@pytest.hookimpl()
def pytest_xdist_make_scheduler(config: pytest.Config, log):
    """Switch to our cool scheduler if --xstress"""
    if not config.getvalue("xstress"):
        return None
    dist = config.getvalue("dist")
    if dist == "loadgroup":
        return LoadGroupStressScheduler(config, log)
    raise ValueError(f'xstress does not support "{dist}" distmode')


@pytest.hookimpl
def pytest_plugin_registered(plugin, manager: pytest.PytestPluginManager):
    """Workaround until PR#1091 in pytest-xdist is merged"""
    if type(plugin).__name__ == "WorkerInteractor":
        # NOTE: Haven't found a better way to get config at this time
        if not plugin.config.getvalue("xstress"):
            return

        # Switch `nextitem=item` to `nextitem=None` when calling `pytest_runtest_protocol`
        func = plugin.config.hook.pytest_runtest_protocol

        @functools.wraps(func)
        def wrapper(item, nextitem, *args, **kwargs):
            if item == nextitem:
                nextitem = None
            func(item=item, nextitem=nextitem, *args, **kwargs)

        plugin.config.hook.pytest_runtest_protocol = wrapper


@pytest.hookimpl()
def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--xstress",
        action="store_true",
        default=False,
        dest="xstress",
        help="run in stress mode (infinite test loop)",
    )
