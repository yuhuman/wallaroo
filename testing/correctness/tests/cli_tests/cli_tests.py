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
                         Runner,
                         RunnerReadyChecker,
                         setup_resilience_path,
                         clean_resilience_path,
                         Sink,
                         start_runners)
import json
import tempfile

Cluster=namedtuple('Cluster', ['host',
                               'n_workers',
                               'ports',
                               'runners',
                               'sink',
                               'metrics',
                               'res_dir'])

def query(host, port, type):
    cmd = "external_sender --json --external {}:{} --type {}"
    return cmd.format(host, port, type)

def test_cluster_status_query():
    cluster = given_cluster()
    cmd = query(cluster.host, cluster.ports[0][2], "cluster-status-query")
    try:
        success, stdout, _, _ = ex_validate(cmd)
        expected = {u"processing_messages": True,
                    u"worker_names": [u"initializer"],
                    u"worker_count": 1}
        assert success
        assert expected == json.loads(stdout)
    finally:
        teardown_cluster(cluster)



def given_cluster(host='127.0.0.1', n_sources=1, n_workers=1,
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
    return Cluster(host=host,
                   n_workers=n_workers,
                   ports=worker_ports,
                   runners=runners,
                   sink=sink, metrics=metrics, res_dir=res_dir)


def teardown_cluster(cluster):
    [ r.stop() for r in cluster.runners ]
    cluster.sink.stop()
    clean_resilience_path(cluster.res_dir)
