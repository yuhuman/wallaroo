# Copyright 2018 The Wallaroo Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.

from collections import namedtuple
from integration import (ex_validate,
                         get_port_values,
                         Metrics,
                         Reader,
                         Runner,
                         RunnerReadyChecker,
                         setup_resilience_path,
                         clean_resilience_path,
                         Sender,
                         sequence_generator,
                         Sink,
                         start_runners)
import json
import tempfile

def test_partition_query():
    with FreshCluster() as cluster:
        q = Query(cluster, "partition-query")
        expected = {"state_partitions":
                    {"Sequence Window": {"initializer": [0,1]}},
                    "stateless_partitions":{}}
        got = q.result()
        assert expected.keys() == got.keys()
        assert got["state_partitions"]["Sequence Window"].has_key("initializer")

def test_partition_count_query():
    with FreshCluster() as cluster:
        q = Query(cluster, "partition-count-query")
        assert q.result() == {
            u"state_partitions": {u"Sequence Window": {u"initializer": 2}},
            u"stateless_partitions": {}}

def test_cluster_status_query():
    with FreshCluster(n_workers=2) as cluster:
        q = Query(cluster, "cluster-status-query")
        assert q.result() == {
            u"processing_messages": True,
            u"worker_names": [u"initializer", u"worker1"],
            u"worker_count": 2}

def test_source_ids_query():
    with FreshCluster(n_sources=1) as cluster:
        q = Query(cluster, "source-ids-query")
        got = q.result()
        assert got.keys() == ["source_ids"]
        assert len(got["source_ids"]) == 1
        assert int(got["source_ids"][0])

def test_boundary_count_status_query():
    with FreshCluster(n_workers=2) as cluster:
        q = Query(cluster, "boundary-count-status", parser=(lambda(x): x))
        # TODO "What do I need to to do get data here?"
        assert q.result() == ""

class FreshCluster(object):
    def __init__(self, host='127.0.0.1', n_sources=1, n_workers=1,
                 command='machida --application-module sequence_window'):
        res_dir = tempfile.mkdtemp(dir='/tmp/', prefix='res-data.')
        setup_resilience_path(res_dir)
        runners = []
        sink = Sink(host)
        sink.start()
        metrics = Metrics(host)
        metrics.start()

        metrics_host, metrics_port = metrics.get_connection_info()
        num_ports = n_sources + 3 * n_workers
        ports = get_port_values(num=num_ports, host=host)
        sink_host, sink_port = sink.get_connection_info()
        outputs = '{}:{}'.format(sink_host, sink_port)
        (input_ports, worker_ports) = (
            ports[:n_sources],
            [ports[n_sources:][i:i+3]
             for i in xrange(0, len(ports[n_sources:]), 3)])
        inputs = ','.join(['{}:{}'.format(host, p) for p in
                           input_ports])
        start_runners(runners, command, host, inputs, outputs,
                      metrics_port, res_dir, n_workers, worker_ports)
        runner_ready_checker = RunnerReadyChecker(runners, timeout=30)
        runner_ready_checker.start()
        runner_ready_checker.join()
        if runner_ready_checker.error:
            raise runner_ready_checker.error
        sender = Sender(host, input_ports[0],
                        Reader(sequence_generator(1000)) ,
                        batch_size=100,
                        interval=0.05)
        sender.start()
        self._cluster = Cluster(host=host,
                                n_workers=n_workers,
                                ports=worker_ports,
                                runners=runners,
                                sink=sink,
                                metrics=metrics,
                                res_dir=res_dir)

    def __enter__(self):
        return self._cluster

    def __exit__(self, type, _value, _traceback):
        if type == Exception:
            print "EX"
            for r in self._cluster.runners:
                print r.get_output()
        [ r.stop() for r in self._cluster.runners ]
        self._cluster.sink.stop()
        clean_resilience_path(self._cluster.res_dir)


Cluster=namedtuple('Cluster', ['host',
                               'n_workers',
                               'ports',
                               'runners',
                               'sink',
                               'metrics',
                               'res_dir'])

class Query(object):
    def __init__(self, cluster, type, parser=json.loads):
        host = cluster.host
        port = cluster.ports[0][2]
        cmd = "external_sender --json --external {}:{} --type {}"
        self._cmd = cmd.format(host, port, type)
        self._parser = parser

    def result(self):
        success, stdout, _, _ = ex_validate(self._cmd)
        if success:
            try:
                return self._parser(stdout)
            except:
                raise Exception("Failed running parser on {!r}".format(stdout))
        else:
            raise Exception("Failed running cmd: {}".format(self._cmd))
