# Copyright 2017 The Wallaroo Authors.
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
import logging
import shlex
import tempfile
import threading
import time
import subprocess


from control import (SinkExpect,
                     SinkAwaitValue,
                     try_until_timeout,
                     wait_for_cluster_to_resume_processing)

from end_points import (Metrics,
                        Sender,
                        Sink)

from errors import (CrashedWorkerError,
                    StopError,
                    TimeoutError)

from external import (clean_resilience_path,
                      get_port_values,
                      send_shrink_command,
                      setup_resilience_path)

from logger import INFO2

from observability import (cluster_status_query,
                           coalesce_partition_query_responses,
                           multi_states_query,
                           ObservabilityNotifier,
                           state_entity_query)

from typed_list import TypedList

from validations import (confirm_migration,
                         worker_count_matches,
                         worker_has_state_entities)

# Make string instance checking py2 and py3 compatible below
try:
    basestring
except:
    basestring = str


class ClusterError(StopError):
    pass


SIGNALS = {"SIGHUP": 1,
           "SIGINT": 2,
           "SIGQUIT": 3,
           "SIGILL": 4,
           "SIGTRAP": 5,
           "SIGABRT": 6,
           "SIGIOT": 6,
           "SIGBUS": 7,
           "SIGFPE": 8,
           "SIGKILL": 9,
           "SIGUSR1": 10,
           "SIGSEGV": 11,
           "SIGUSR2": 12,
           "SIGPIPE": 13,
           "SIGALRM": 14,
           "SIGTERM": 15,
           "SIGSTKFLT": 16,
           "SIGCHLD": 17,
           "SIGCONT": 18,
           "SIGSTOP": 19,
           "SIGTSTP": 20,
           "SIGTTIN": 21,
           "SIGTTOU": 22,
           "SIGURG": 23,
           "SIGXCPU": 24,
           "SIGXFSZ": 25,
           "SIGVTALRM": 26,
           "SIGPROF": 27,
           "SIGWINCH": 28,
           "SIGIO": 29,
           "SIGPOLL": 29,
           "SIGLOST": 29,
           "SIGPWR": 30,
           "SIGSYS": 31,
           "SIGUNUSED": 31,}

class Runner(threading.Thread):
    """
    Run a shell application with command line arguments and save its stdout
    and stderr to a temporoary file.

    `get_output` may be used to get the entire output text up to the present,
    as well as after the runner has been stopped via `stop()`.
    """
    def __init__(self, command, name, control=None, data=None, external=None):
        super(Runner, self).__init__()
        self.daemon = True
        self.command = ' '.join(v.strip('\\').strip() for v in
                                  command.splitlines())
        self.cmd_args = shlex.split(self.command)
        self.error = None
        self.file = tempfile.NamedTemporaryFile()
        self.p = None
        self.pid = None
        self.name = name
        self.control = control
        self.data = data
        self.external = external

    def run(self):
        try:
            logging.log(INFO2, "{}: Running:\n{}".format(self.name,
                                                         self.command))
            self.p = subprocess.Popen(args=self.cmd_args,
                                      stdout=self.file,
                                      stderr=subprocess.STDOUT)
            self.pid = self.p.pid
            self.p.wait()
        except Exception as err:
            self.error = err
            logging.warn("{}: Stopped running!".format(self.name))
            raise

    def send_signal(self, signal):
        logging.debug("send_signal(signal={})".format(signal))
        self.p.send_signal(signal)

    def stop(self):
        logging.debug("stop()")
        try:
            self.p.terminate()
        except:
            pass

    def kill(self):
        logging.debug("kill()")
        try:
            self.p.kill()
        except:
            pass

    def is_alive(self):
        logging.debug("is_alive()")
        if self.p is None:
            raise ValueError("Runner hasn't started yet.")
        status = self.p.poll()
        if status is None:
            return True
        else:
            return False

    def poll(self):
        logging.debug("poll()")
        if self.p is None:
            raise ValueError("Runner hasn't started yet.")
        return self.p.poll()

    def returncode(self):
        logging.debug("returncode()")
        if self.p is None:
            raise ValueError("Runner hasn't started yet.")
        return self.p.returncode

    def get_output(self, start_from=0):
        logging.debug("get_output(start_from={})".format(start_from))
        self.file.flush()
        with open(self.file.name, 'rb') as ro:
            ro.seek(start_from)
            return ro.read()

    def tell(self):
        logging.debug("tell()")
        """
        Return the STDOUT file's current position
        """
        return self.file.tell()

    def respawn(self):
        logging.debug("respawn()")
        return Runner(self.command, self.name, control=self.control,
                      data=self.data, external=self.external)


BASE_COMMAND = r'''{command} \
    --in {inputs} \
    --out {outputs} \
    --metrics {metrics_addr} \
    --resilience-dir {res_dir} \
    --name {{name}} \
    {{initializer_block}} \
    {{worker_block}} \
    {{join_block}} \
    {{spike_block}} \
    {{alt_block}} \
    --ponythreads=1 \
    --ponypinasio \
    --ponynoblock'''
INITIALIZER_CMD = r'''{worker_count} \
    --data {data_addr} \
    --external {external_addr} \
    --cluster-initializer'''
WORKER_CMD = r'''
    --my-control {control_addr} \
    --my-data {data_addr} \
    --external {external_addr}'''
JOIN_CMD = r'''--join {join_addr} \
    {worker_count}'''
CONTROL_CMD = r'''--control {control_addr}'''
WORKER_COUNT_CMD = r'''--worker-count {worker_count}'''
SPIKE_CMD = r'''--spike-drop \
    {prob} \
    {margin} \
    {seed}'''
SPIKE_SEED = r'''--spike-seed {seed}'''
SPIKE_PROB = r'''--spike-prob {prob}'''
SPIKE_MARGIN = r'''--spike-margin {margin}'''


def start_runners(runners, command, source_addrs, sink_addrs, metrics_addr,
                  res_dir, workers, worker_addrs=[], alt_block=None,
                  alt_func=lambda x: False, spikes={}):
    cmd_stub = BASE_COMMAND.format(command=command,
                                   inputs=','.join(source_addrs),
                                   outputs=','.join(sink_addrs),
                                   metrics_addr=metrics_addr,
                                   res_dir=res_dir)

    # for each worker, assign `name` and `cluster-initializer` values
    if workers < 1:
        raise ClusterError("workers must be 1 or more")
    x = 0
    if x in spikes:
        logging.info("Enabling spike for initializer")
        sc = spikes[x]
        spike_block = SPIKE_CMD.format(
            prob=SPIKE_PROB.format(prob=sc.probability),
            margin=SPIKE_MARGIN.format(margin=sc.margin),
            seed=SPIKE_SEED.format(seed=sc.seed) if sc.seed else '')
    else:
        spike_block = ''
    cmd = cmd_stub.format(
        name='initializer',
        initializer_block=INITIALIZER_CMD.format(
            worker_count=WORKER_COUNT_CMD.format(worker_count=workers),
            data_addr=worker_addrs[0][1],
            external_addr=worker_addrs[0][2]),
        worker_block='',
        join_block=CONTROL_CMD.format(control_addr=worker_addrs[0][0]),
        alt_block=alt_block if alt_func(x) else '',
        spike_block=spike_block)
    runners.append(Runner(command=cmd, name='initializer',
                          control=worker_addrs[0][0],
                          data=worker_addrs[0][1],
                          external=worker_addrs[0][2]))
    for x in range(1, workers):
        if x in spikes:
            logging.info("Enabling spike for worker{}".format(x))
            sc = spikes[x]
            spike_block = SPIKE_CMD.format(
                prob=SPIKE_PROB.format(prob=sc.probability),
                margin=SPIKE_MARGIN.format(margin=sc.margin),
                seed=SPIKE_SEED.format(seed=sc.seed) if sc.seed else '')
        else:
            spike_block = ''
        cmd = cmd_stub.format(name='worker{}'.format(x),
                              initializer_block='',
                              worker_block=WORKER_CMD.format(
                                  control_addr=worker_addrs[x][0],
                                  data_addr=worker_addrs[x][1],
                                  external_addr=worker_addrs[x][2]),
                              join_block=CONTROL_CMD.format(
                                  control_addr=worker_addrs[0][0]),
                              alt_block=alt_block if alt_func(x) else '',
                              spike_block=spike_block)
        runners.append(Runner(command=cmd,
                              name='worker{}'.format(x),
                              control=worker_addrs[x][0],
                              data=worker_addrs[x][1],
                              external=worker_addrs[x][2]))

    # start the workers, 50ms apart
    for idx, r in enumerate(runners):
        r.start()
        time.sleep(0.05)

        # check the runners haven't exited with any errors
        try:
            assert(r.is_alive())
        except Exception as err:
            stdout = r.get_output()
            raise ClusterError(
                    "Runner %d of %d has exited with an error: "
                    "\n---\n%s" % (idx+1, len(runners), stdout))
        try:
            assert(r.error is None)
        except Exception as err:
            raise ClusterError(
                    "Runner %d of %d has exited with an error: "
                    "\n---\n%s" % (idx+1, len(runners), r.error))


def add_runner(worker_id, runners, command, source_addrs, sink_addrs, metrics_addr,
               control_addr, res_dir, workers,
               my_control_addr, my_data_addr, my_external_addr,
               alt_block=None, alt_func=lambda x: False, spikes={}):
    cmd_stub = BASE_COMMAND.format(command=command,
                                   inputs=','.join(source_addrs),
                                   outputs=','.join(sink_addrs),
                                   metrics_addr=metrics_addr,
                                   res_dir=res_dir)

    # Test that the new worker *can* join
    if len(runners) < 1:
        raise ClusterError("There must be at least 1 worker to join!")

    if not any(r.is_alive() for r in runners):
        raise ClusterError("There must be at least 1 live worker to "
                                "join!")

    if worker_id in spikes:
        logging.info("Enabling spike for joining worker{}".format(x))
        sc = spikes[worker_id]
        spike_block = SPIKE_CMD.format(
            prob=SPIKE_PROB.format(sc.probability),
            margin=SPIKE_MARGIN.format(sc.margin),
            seed=SPIKE_SEED.format(sc.seed) if sc.seed else '')
    else:
        spike_block = ''

    cmd = cmd_stub.format(name='worker{}'.format(worker_id),
                          initializer_block='',
                          worker_block=WORKER_CMD.format(
                              control_addr=my_control_addr,
                              data_addr=my_data_addr,
                              external_addr=my_external_addr),
                          join_block=JOIN_CMD.format(
                              join_addr=control_addr,
                              worker_count=(WORKER_COUNT_CMD.format(
                                  worker_count=workers) if workers else '')),
                          alt_block=alt_block if alt_func(worker_id) else '',
                          spike_block=spike_block)
    runner = Runner(command=cmd,
                    name='worker{}'.format(worker_id),
                    control=my_control_addr,
                    data=my_data_addr,
                    external=my_external_addr)
    runners.append(runner)

    # start the new worker
    runner.start()
    time.sleep(0.05)

    # check the runner hasn't exited with any errors
    try:
        assert(runner.is_alive())
    except Exception as err:
        raise CrashedWorkerError
    try:
        assert(runner.error is None)
    except Exception as err:
        raise err
    return runner


RunnerData = namedtuple('RunnerData',
                        ['name',
                         'command',
                         'pid',
                         'returncode',
                         'stdout'])


class Cluster(object):
    def __init__(self, command, host='127.0.0.1', sources=1, workers=1,
            sinks=1, sink_mode='framed', worker_join_timeout=30,
            is_ready_timeout=30, res_dir=None, runner_data=[]):
        # Create attributes
        self._finalized = False
        self.command = command
        self.host = host
        self.workers = TypedList(types=(Runner,))
        self.dead_workers = TypedList(types=(Runner,))
        self.runners = TypedList(types=(Runner,))
        self.source_addrs = []
        self.sink_addrs = []
        self.sinks = []
        self.senders = []
        self.worker_join_timeout = worker_join_timeout
        self.is_ready_timeout = is_ready_timeout
        self.metrics = Metrics(host, mode='framed')
        self.errors = []
        self._worker_id_counter = 0
        if res_dir is None:
            self.res_dir = tempfile.mkdtemp(dir='/tmp/', prefix='res-data.')
        else:
            self.res_dir = res_dir
        self.runner_data = runner_data

        # Try to start everything... clean up on exception
        try:
            setup_resilience_path(self.res_dir)

            self.metrics.start()
            self.metrics_addr = ":".join(
                map(str, self.metrics.get_connection_info()))

            for s in range(sinks):
                self.sinks.append(Sink(host, mode=sink_mode))
                self.sinks[-1].start()
                if self.sinks[-1].err is not None:
                    raise self.sinks[-1].err

            self.sink_addrs = ["{}:{}"
                               .format(*map(str,s.get_connection_info()))
                               for s in self.sinks]

            num_ports = sources + 3 * workers
            ports = get_port_values(num=num_ports, host=host)
            addresses = ['{}:{}'.format(host, p) for p in ports]
            (self.source_addrs, worker_addrs) = (
                addresses[:sources],
                [addresses[sources:][i:i+3]
                 for i in xrange(0, len(addresses[sources:]), 3)])
            start_runners(self.workers, self.command, self.source_addrs,
                          self.sink_addrs,
                          self.metrics_addr, self.res_dir, workers,
                          worker_addrs)
            self.runners.extend(self.workers)
            self._worker_id_counter = len(self.workers)

            # Wait for all runners to report ready to process
            self.wait_to_resume_processing(self.is_ready_timeout)
            # make sure `workers` runners are active and listed in the
            # cluster status query
            logging.debug("Testing cluster size via obs query")
            self.query_observability(cluster_status_query,
                                     self.runners[0].external,
                                     tests=[(worker_count_matches, [workers])])
        except Exception as err:
            logging.error("Encountered and error when starting up the cluster")
            logging.exception(err)
            self.errors.append(err)
            self.__finally__()
            raise err

    #############
    # Autoscale #
    #############
    def grow(self, by=1, timeout=30, with_test=True):
        logging.debug("grow(by={}, timeout={}, with_test={})".format(
            by, timeout, with_test))
        pre_partitions = self.get_partition_data() if with_test else None
        runners = []
        new_ports = get_port_values(num = 3 * by,
                                    host = self.host,
                                    base_port=25000)
        addrs = [["{}:{}".format(self.host, p) for p in new_ports[i*3: i*3 + 3]]
                 for i in range(by)]
        for x in range(by):
            runner = add_runner(
                worker_id=self._worker_id_counter,
                runners=self.workers,
                command=self.command,
                source_addrs=self.source_addrs,
                sink_addrs=self.sink_addrs,
                metrics_addr=self.metrics_addr,
                control_addr=self.workers[0].control,
                res_dir=self.res_dir,
                workers=by,
                my_control_addr=addrs[x][0],
                my_data_addr=addrs[x][1],
                my_external_addr=addrs[x][2])
            self._worker_id_counter += 1
            runners.append(runner)
            self.runners.append(runner)
        if with_test:
            workers = {'joining': [w.name for w in runners],
                       'leaving': []}
            self.confirm_migration(pre_partitions, workers)
        return runners

    def shrink(self, workers=1, with_test=True):
        logging.debug("shrink(workers={}, with_test={})".format(
            workers, with_test))
        # pick a worker that's not being shrunk
        if isinstance(workers, basestring):
            snames = set(workers.split(","))
            wnames = set([w.name for w in self.workers])
            complement = wnames - snames # all members of wnames not in snames
            if not complement:
                raise ValueError("Can't shrink all workers!")
            for w in self.workers:
                if w.name in complement:
                    address = w.external
                    break
            leaving = filter(lambda w: w.name in snames, self.workers)
        elif isinstance(workers, int):
            if len(self.workers) <= workers:
                raise ValueError("Can't shrink all workers!")
            # choose last workers in `self.workers`
            leaving = self.workers[-workers:]
            address = self.workers[0].external
        else:
            raise ValueError("shrink(workers): workers must be an int or a"
                " comma delimited string of worker names.")
        names = ",".join((w.name for w in leaving))
        pre_partitions = self.get_partition_data() if with_test else None
        # send shrink command to non-shrinking worker
        logging.debug("Sending a shrink command for ({})".format(names))
        resp = send_shrink_command(address, names)
        logging.debug("Response was: {}".format(resp))
        # no error, so command was successful, update self.workers
        for w in leaving:
            self.workers.remove(w)
        self.dead_workers.extend(leaving)
        if with_test:
            workers = {'leaving': [w.name for w in leaving],
                       'joining': []}
            self.confirm_migration(pre_partitions, workers)
        return leaving

    def get_partition_data(self):
        logging.debug("get_partition_data()")
        addresses = [(w.name, w.external) for w in self.workers]
        responses = multi_states_query(addresses)
        return coalesce_partition_query_responses(responses)

    def confirm_migration(self, pre_partitions, workers, timeout=120):
        logging.debug("confirm_migration(pre_partitions={}, workers={},"
            " timeout={})".format(pre_partitions, workers, timeout))
        def pre_process():
            addresses = [(r.name, r.external) for r in self.workers]
            responses = multi_states_query(addresses)
            post_partitions = coalesce_partition_query_responses(responses)
            return (pre_partitions, post_partitions, workers)
        # retry the test until it passes or a timeout elapses
        logging.debug("Running pre_process func with try_until")
        try_until_timeout(confirm_migration, pre_process, timeout=120)

    #####################
    # Worker management #
    #####################
    def kill_worker(self, worker=-1):
        """
        Kill a worker, move it from `workers` to `dead_workers`, and return
        it.
        If `worker` is an int, perform this on the Runner instance at `worker`
        position in the `workers` list.
        If `worker` is a Runner instance, perform this on that instance.
        """
        logging.debug("kill_worker(worker={})".format(worker))
        if isinstance(worker, Runner):
            # ref to worker
            r = self.workers.remove(worker)
        else: # index of worker in self.workers
            r = self.workers.pop(worker)
        r.kill()
        self.dead_workers.append(r)
        return r

    def stop_worker(self, worker=-1):
        """
        Stop a worker, move it from `workers` to `dead_workers`, and return
        it.
        If `worker` is an int, perform this on the Runner instance at `worker`
        position in the `workers` list.
        If `worker` is a Runner instance, perform this on that instance.
        """
        logging.debug("stop_worker(worker={})".format(worker))
        if isinstance(worker, Runner):
            # ref to worker
            r = self.workers.remove(worker)
        else: # index of worker in self.workers
            r = self.workers.pop(worker)
        r.stop()
        self.dead_workers.append(r)
        return r

    def restart_worker(self, worker=-1):
        """
        Restart a worker via the `respawn` method of a runner, then add the
        new Runner instance to `workers`.
        If `worker` is an int, perform this on the Runner instance at `worker`
        position in the `dead_workers` list.
        If `worker` is a Runner instance, perform this on that instance.
        """
        logging.debug("restart_worker(worker={})".format(worker))
        if isinstance(worker, Runner):
            # ref to dead worker instance
            old_r = worker
        else: # index of worker in self.dead_workers
            old_r = self.dead_workers[worker]
        r = old_r.respawn()
        self.workers.append(r)
        self.runners.append(r)
        r.start()
        return r

    def stop_workers(self):
        logging.debug("stop_workers()")
        for r in self.runners:
            r.stop()
        # move all live workers to dead_workers
        self.dead_workers.extend(self.workers)
        self.workers = []

    def get_crashed_workers(self):
        logging.debug("get_crashed_workers()")
        return filter(lambda r: r.poll() not in (None, 0,-9,-15), self.runners)

    #########
    # Sinks #
    #########
    def stop_sinks(self):
        logging.debug("stop_sinks()")
        for s in self.sinks:
            s.stop()

    def sink_await(self, values, timeout=30, sink=-1):
        logging.debug("sink_await(values={}, timeout={}, sink={})".format(
            values, timeout, sink))
        if isinstance(sink, Sink):
            pass
        else:
            sink = self.sinks[sink]
        t = SinkAwaitValue(sink, values, timeout)
        t.start()
        t.join()
        if t.error:
            raise t.error
        return sink

    def sink_expect(self, expected, timeout=30, sink=-1):
        logging.debug("sink_expect(expected={}, timeout={}, sink={})".format(
            expected, timeout, sink))
        if isinstance(sink, Sink):
            pass
        else:
            sink = self.sinks[sink]
        t = SinkExpect(sink, expected, timeout)
        t.start()
        t.join()
        if t.error:
            raise t.error
        return sink

    ###########
    # Senders #
    ###########
    def add_sender(self, sender, start=False):
        logging.debug("add_sender(sender={}, start={})".format(sender, start))
        self.senders.append(sender)
        if start:
            sender.start()

    def wait_for_sender(self, sender=-1, timeout=30):
        logging.debug("wait_for_sender(sender={}, timeout={})"
            .format(sender, timeout))
        if isinstance(sender, Sender):
            pass
        else:
            sender = self.senders[sender]
        sender.join(timeout)
        if sender.error:
            raise sender.error
        if sender.is_alive():
            raise TimeoutError('Sender did not complete in the expected '
                               'period')

    def stop_senders(self):
        logging.debug("stop_senders()")
        for s in self.senders:
            s.stop()

    def pause_senders(self):
        logging.debug("pause_senders()")
        for s in self.senders:
            s.pause()

    def resume_senders(self):
        logging.debug("resume_senders()")
        for s in self.senders:
            s.resume()

    ###########
    # Cluster #
    ###########
    def wait_to_resume_processing(self, timeout=30):
        logging.debug("wait_to_resume_processing(timeout={})"
            .format(timeout))
        wait_for_cluster_to_resume_processing(self.workers, timeout=timeout)

    def stop_cluster(self):
        logging.debug("stop_cluster()")
        self.stop_senders()
        self.stop_workers()
        self.stop_sinks()

    #########################
    # Observability queries #
    #########################
    def query_observability(self, query, args, tests, timeout=30, period=2):
        logging.debug("query_observability(query={}, args={}, tests={}, "
            "timeout={}, period={})".format(
                query, args, tests, timeout, period))
        obs = ObservabilityNotifier(query, args, tests, timeout, period)
        obs.start()
        obs.join()
        if obs.error:
            raise obs.error

    ###########################
    # Context Manager functions:
    ###########################
    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        logging.debug("__exit__(...)")
        # clean up any remaining runner processes
        try:
            for w in self.workers:
                w.stop()
            for w in self.dead_workers:
                w.stop()
            # Wait on runners to finish waiting on their subprocesses to exit
            for w in self.runners:
                # Check thread ident to avoid error when joining an un-started
                # thread.
                if w.ident:  # ident is set when a thread is started
                    w.join(self.worker_join_timeout)
            alive = []
            while self.workers:
                w = self.workers.pop()
                if w.is_alive():
                    alive.append(w)
                else:
                    self.dead_workers.append(w)
            if alive:
                alive_names = ', '.join((w.name for w in alive))
                raise ClusterError("Runners [{}] failed to exit cleanly after"
                                        " {} seconds.".format(alive_names))
            # check for workes that exited with a non-0 return code
            # note that workers killed in the previous step have code -15
            bad_exit = []
            for w in self.dead_workers:
                c = w.returncode()
                if c not in (0,-9,-15):  # -9: SIGKILL, -15: SIGTERM
                    bad_exit.append(w)
            if bad_exit:
                for w in bad_exit:
                    logging.error("Runner {} exited with return code {}"
                        .format(w.name, w.returncode()))
                raise ClusterError("The following workers terminated with "
                    "a bad exit code: {}"
                    .format(["(name: {}, pid: {}, code:{})".format(
                        w.name, w.pid, w.returncode())
                             for w in bad_exit]))
        finally:
            self.__finally__()
            if self.errors:
                 logging.error("Errors were encountered when running"
                    " the cluster")
                 for e in self.errors:
                     logging.exception(e)

    def __finally__(self):
        logging.debug("__finally__()")
        if self._finalized:
            return
        logging.info("Doing final cleanup")
        for w in self.runners:
            w.kill()
        for s in self.sinks:
            s.stop()
        for s in self.senders:
            s.stop()
        self.metrics.stop()
        clean_resilience_path(self.res_dir)
        self.runner_data.extend([RunnerData(r.name, r.command, r.pid,
                                            r.returncode(), r.get_output())
                                 for r in self.runners])
        self._finalized = True


def runner_data_format(runner_data, from_tail=0,
                       filter_fn=lambda r: r.returncode not in (0,-9,-15)):
    """
    Format runners' STDOUTs for printing or for inclusion in an error
    - `runners` is a list of Runner instances
    - `from_tail` is the number of lines to keep from the tail of each
      runner's log. The default is to keep the entire log.
    - `filter_fn` is a function to apply to each runner to determine
      whether or not to include its output in the log. The function
      should return `True` if the runner's output is to be included.
      Default is to keep all outputs.
    """
    return '\n===\n'.join(
        ('{identifier} ->\n\n{stdout}'
         '\n\n{identifier} <-'
         .format(identifier="--- {name} (pid: {pid}, rc: {rc})".format(
                    name=rd.name,
                    pid=rd.pid,
                    rc=rd.returncode),
                 stdout='\n'.join(rd.stdout.splitlines()[-from_tail:]))
         for rd in runner_data
         if filter_fn(rd)))
