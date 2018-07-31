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


# import requisite components for integration test
from integration import (
                         clean_resilience_path,
                         ex_validate,
                         get_port_values,
                         Metrics,
                         MultiSequenceGenerator,
                         PipelineTestError,
                         Reader,
                         Runner,
                         RunnerReadyChecker,
                         runners_output_format,
                         Sender,
                         setup_resilience_path,
                         Sink,
                         SinkAwaitValue,
                         start_runners,
                         TimeoutError)

import logging
from integration import INFO2, set_logging
set_logging(name='autoscale_tests', level=logging.INFO,
            fmt='%(levelname)s - %(message)s')

import os
import re
import struct
import tempfile
import time


def test_recovery_concurrent_pony():
    command = 'multi_partition_detector'
    _test_recovery(command, base_workers=2, first=1, delay=30, second=1)


def _test_recovery(command, base_workers=2, first=1, delay=1, second=0):
    try:
        _test_recovery_main(command, base_workers, first, delay, second)
    except Exception as err:
        if hasattr(err, 'runners'):
            if filter(lambda r: r.poll() != 0, err.runners):
                outputs = runners_output_format(err.runners,
                        from_tail=10, filter_fn=lambda r: r.poll() != 0)
                print("Some runners exited badly. "
                      "They had the following "
                      "output tails:\n===\n{}".format(outputs))
        raise


def _test_recovery_main(command, base_workers=2, first=1, delay=1, second=0):
    """
    Run a cluster of size `workers`, and kill `first` as the first group
    and `second` as the staggered group.
    e.g. with `workers=3, first=1, second=1`, a 3-worker cluster we be
    created, with one worker killed, then another killed during while
    the first is recovering.
    """
    host = '127.0.0.1'
    sources = 1
    workers = base_workers + first + second
    partition_multiplier = 5  # Used in partition count creation
    res_dir = tempfile.mkdtemp(dir='/tmp/', prefix='res-data.')
    runner_join_timeout = 30
    setup_resilience_path(res_dir)
    # create the sequence generator and the reader
    msg = MultiSequenceGenerator()

    runners = []
    try:
        try:
            # Create sink, metrics, reader, sender
            sink = Sink(host)
            metrics = Metrics(host)
            reader = Reader(msg)

            # Start sink and metrics, and get their connection info
            sink.start()
            sink_host, sink_port = sink.get_connection_info()
            outputs = '{}:{}'.format(sink_host, sink_port)

            metrics.start()
            metrics_host, metrics_port = metrics.get_connection_info()
            time.sleep(0.05)

            num_ports = sources + 3 * workers
            ports = get_port_values(num=num_ports, host=host)
            (input_ports, worker_ports) = (
                ports[:sources],
                [ports[sources:][i:i+3] for i in xrange(0,
                    len(ports[sources:]), 3)])
            inputs = ','.join(['{}:{}'.format(host, p) for p in
                               input_ports])

            start_runners(runners, command, host, inputs, outputs,
                          metrics_port, res_dir, workers, worker_ports)

            # Wait for first runner (initializer) to report application ready
            runner_ready_checker = RunnerReadyChecker(runners, timeout=30)
            runner_ready_checker.start()
            runner_ready_checker.join()
            if runner_ready_checker.error:
                raise runner_ready_checker.error

            # start sender
            sender = Sender(host, input_ports[0], reader, batch_size=10,
                            interval=0.05)
            sender.start()
            time.sleep(1)

            # bring partition count to (workers + first + second) * partition_multiplier
            for x in range(workers * partition_multiplier - 1):
                msg.add_sequence()

            # crash the first batch
            first_killed = []
            for x in range(1, first + 1):
                runners[x].kill()
                first_killed.append(runners[x])

            # restart first batch
            first_recovered = []
            for r in first_killed:
                first_recovered.append(r.respawn())
                runners.append(first_recovered[-1])
            for r in first_recovered:
                r.start()

            # wait an arbitrary amount of time: 1 second
            time.sleep(delay)

            # crash second batch
            second_killed = []
            for x in range(first + 1, first + 1 + second):
                runners[x].kill()
                second_killed.append(runners[x])

            # restart second batch
            second_recovered = []
            for r in second_killed:
                second_recovered.append(r.respawn())
                runners.append(second_recovered[-1])
            for r in second_recovered:
                r.start()


            # Give things some time to settle
            time.sleep(10)

            # Tell the multi-sequence-sender to stop
            msg.stop()
            # Then wait for the sender to finish sending
            sender.join(30)
            if sender.error:
                raise sender.error
            if sender.is_alive():
                sender.stop()
                raise TimeoutError('Sender did not complete in the expected '
                                   'period')

            # Create await_values for the sink based on the stop values from the
            # multi sequence generator
            await_values = []
            for part, val in enumerate(msg.seqs):
                key = '{:07d}.TraceID-1.TraceWindow-1.TraceID-2.TraceWindow-2'.format(part)
                data = '[{},{},{},{}]'.format(*[val-x for x in range(3,-1,-1)])
                s = '({},{})'.format(key, data)
                await_values.append(struct.pack('>I', len(s)) + s)
            # Use metrics to determine when to stop runners and sink
            stopper = SinkAwaitValue(sink, await_values, 30)
            stopper.start()
            stopper.join()
            if stopper.error:
                #print 'sink.data', sink.data
                raise stopper.error
            logging.info("Completion condition achieved. Shutting down cluster.")

            # stop application workers
            for r in runners:
                r.stop()

            # Stop sink
            sink.stop()
            #print 'sink.data size: ', len(sink.data)

            # Use validator to validate the data in at-least-once mode
            # save sink data to a file
            out_file = os.path.join(res_dir, 'received.txt')
            sink.save(out_file)


            # Validate captured output
            logging.info("Validating output")
            cmd_validate = ('validator -i {out_file} -e {expect} -a'
                            .format(out_file = out_file,
                                    expect = msg.stop_value))
            success, stdout, retcode, cmd = ex_validate(cmd_validate)
            try:
                assert(success)
                logging.info("Validation successful")
            except:
                raise AssertionError('Validation failed with the following '
                                     'error:\n{}'.format(stdout))

            # Validate worker actually underwent recovery
            logging.info("Validating recovery")
            pattern = "RESILIENCE\: Replayed \d+ entries from recovery log file\."
            recovered = []
            recovered.extend(first_recovered)
            recovered.extend(second_recovered)
            for r in recovered:
                stdout = r.get_output()
                try:
                    assert(re.search(pattern, stdout) is not None)
                    logging.info("{} recovered successfully".format(r.name))
                except AssertionError:
                    raise AssertionError('Worker does not appear to have performed '
                                         'recovery as expected. Worker output is '
                                         'included below.\nSTDOUT\n---\n%s'
                                         % stdout)

        finally:
            logging.info("Doing final cleanup")
            # clean up any remaining runner processes
            for r in runners:
                r.stop()
            # Wait on runners to finish waiting on their subprocesses to exit
            for r in runners:
                # Check thread ident to avoid error when joining an un-started
                # thread.
                if r.ident:  # ident is set when a thread is started
                    r.join(runner_join_timeout)
            alive = []
            for r in runners:
                if r.is_alive():
                    alive.append(r)
            if alive:
                alive_names = ', '.join((r.name for r in alive))
                outputs = runners_output_format(runners)
                for a in alive:
                    logging.info("Runner {} is still alive. Sending SIGKILL."
                        .format(a.name))
                    a.kill()
            clean_resilience_path(res_dir)
            if alive:
                raise Exception("Runners [{}] failed to exit cleanly after"
                                        " {} seconds.\n"
                                        "Runner outputs are attached below:"
                                        "\n===\n{}"
                                        .format(alive_names, runner_join_timeout,
                                                outputs))
            # check for workes that exited with a non-0 return code
            # note that workers killed in the previous step have code -15
            bad_exit = []
            for r in runners:
                c = r.returncode()
                if c not in (0,-9,-15):  # -9: SIGKILL, -15: SIGTERM
                    bad_exit.append(r)
            if bad_exit:
                for r in bad_exit:
                    logging.warn("Runner {} exited with return code {}"
                        .format(r.name, r.returncode()))
                raise Exception("The following workers terminated with "
                    "a bad exit code: {}"
                    .format(["{} ({})".format(r.name, r.returncode())
                             for r in bad_exit]))

    except Exception as err:
        if not hasattr(err, 'runners'):
            err.runners = runners
        raise
