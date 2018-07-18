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
from integration import (Cluster,
                         run_shell_cmd,
                         MultiSequenceGenerator,
                         Reader,
                         runner_data_format,
                         Sender)
from integration.logger import set_logging

import logging
from numbers import Number
import os
import re
import time

try:
    basestring
except:
    basestring = str

set_logging()

FROM_TAIL = int(os.environ.get("FROM_TAIL", 10))


##################
# Helper Functions
##################

# Keep only the key as a string, and the final output tuple as a
# string
def parse_sink_value(s):
    return (s[4:].strip("()").split(',',1)[0].split(".",1)[0],
        s[4:].strip("()").split(",",1)[1])


# TODO: refactor and move to control.py
def pause_senders_and_sink_await(cluster, timeout=10):
    logging.debug("pause_senders_and_sink_await")
    cluster.pause_senders()
    logging.debug("Waiting for messages to propagate to sink")
    msg = cluster.senders[0].reader.gen
    await_values = []
    for part, val in enumerate(msg.seqs):
        key = '{:07d}'.format(part)
        data = '[{},{},{},{}]'.format(*[val-x for x in range(3,-1,-1)])
        await_values.append((key, data))
    cluster.sink_await(values=await_values, func=parse_sink_value)
    logging.debug("All messages arrived at sink!")


class ResilienceOperation(object):
    """
    Baseclass for resilience operation types
    """
    def __init__(self, size, check_size=True):
        if check_size:
            if isinstance(size, int):
                if size <= 0:
                    raise ValueError("size must be a positive integer")
            else:
                raise TypeError("size must be an integer")
        self.size = size

    def sign(self):
        """
        Identify the sign of the operation: e.g. whether it adds or subtracts
        from the total size of the cluster
        >0: 1
        0: 0
        <1: -1
        """
        raise NotImplementedError

    def apply(self, cluster, data=None):
        """
        The logic involved in applying this step and any direct validations
        """
        raise NotImplementedError

    def name(self):
        """
        The name of of the operation, along with its size.
        Useful when printing a history of operations in a concise manner.
        """
        return "{}:{}".format(self.__class__.__name__, self.size)

    def __str__(self):
        return self.name()


class Grow(ResilienceOperation):
    def __init__(self, size, timeout=30, with_test=True):
        super(Grow, self).__init__(size)
        self.timeout = timeout
        self.with_test = with_test

    def sign(self):
        return 1

    def apply(self, cluster, data=None):
        return cluster.grow(by=self.size, timeout=self.timeout,
                            with_test=self.with_test)


class Shrink(ResilienceOperation):
    def __init__(self, workers, timeout=30, with_test=True):
        self.workers = workers
        self.timeout = timeout
        self.with_test = with_test
        if isinstance(workers, basestring):
            super(Shrink, self).__init__(len(workers.split(',')))
        else:
            super(Shrink, self).__init__(workers)

    def sign(self):
        return -1

    def apply(self, cluster, data=None):
        return cluster.shrink(self.workers, timeout=self.timeout,
                              with_test=self.with_test)


class Crash(ResilienceOperation):
    def __init__(self, workers):
        if isinstance(workers, (tuple, list)):
            super(Crash, self).__init__(len(workers))
            self.slice = slice(-1, None, 1)
        else:
            super(Crash, self).__init__(workers)
            self.slice = slice(-workers, None, 1)

    def sign(self):
        return -1

    def apply(self, cluster, data=None):
        pause_senders_and_sink_await(cluster)
        killed = []
        for w in cluster.workers[self.slice]:
            logging.debug("w is: {}".format(w))
            killed.append(cluster.kill_worker(w))
        return killed


class Recover(ResilienceOperation):
    def __init__(self, size=None, timeout=30, with_test=True):
        """
        :size may be a positive int or None
        if int, it denotes the number of workers to recover from the tail end
        of cluster.dead_workers.
        if None, it means recover all workers from the previous step.
        """
        super(Recover, self).__init__(size, check_size=False)
        self.timeout = timeout
        self.with_test = with_test

    def sign(self):
        return 1

    def apply(self, cluster, workers=[]):
        if self.size:
            size = self.size
            # continue below
        elif workers:
            if isinstance(workers, Runner):
                return [cluster.restart_worker(workers)]
            elif isinstance(workers, (tuple, list)):
                return [cluster.restart_worker(w) for w in workers]
            elif isinstance(workers, int):
                size = workers
                # continue below
        else:
            raise ValueError("size or workers must be defined")
        killed = cluster.dead_workers[-size:]
        restarted = cluster.restart_worker(killed)
        if self.with_test:
            cluster.wait_to_resume_processing(timeout=self.timeout)
        cluster.resume_senders()
        return restarted


class Wait(ResilienceOperation):
    def __init__(self, seconds):
        if not isinstance(seconds, Number):
            raise TypeError("seconds must be a number")
        super(Wait, self).__init__(seconds, check_size=False)

    def sign(self):
        return 0

    def apply(self, cluster, data=None):
        time.sleep(self.size)
        return data


def lowest_point(ops):
    l = None
    p = 0
    last_size = None
    for o in ops:
        if isinstance(o, Recover):
            if o.size:
                size = o.size
            else:
                size = last_size
        else:
            size = o.size
        p += (size * o.sign())
        if l is None:
            l  = p
        if p < l:
            l = p
    return l


####################################
# Test Runner - Error handler wrapper
####################################


def _test_resilience(command, ops=[], initial=1, sources=1,
                     partition_multiplier=5, cycles=1, validate_output=True):
    """
    Execute a resilience test for the given command.

    `command` - the command string to execute
    `ops` - the list of operations to perform.
    `initial` - (optional) the initial cluster size
    `sources` - the number of sources to use
    `partition_multiplier` - multiply number of workers by this to determine
      how many partitiosn to use
    `cycles` - how many times to repeat the list of operations
    `validate_output` - whether or not to validate the output
    """

    runner_data = []
    res_ops = []
    try:
        _run(runner_data, res_ops, command, ops*cycles, initial, sources,
             partition_multiplier)
    except:
        logging.error("Resilience test encountered an error after the steps"
                      " {}".format([o.name() for o in res_ops]))
        if runner_data:
            logging.error("Some workers exited badly. The last {} lines of "
                "each were:\n\n{}"
                .format(FROM_TAIL,
                    runner_data_format(runner_data,
                                       from_tail=FROM_TAIL)))
        raise


#############
# Test Runner
#############

def _run(runner_data, res_ops, command, ops=[], initial=None, sources=1,
         partition_multiplier=1, validate_output=True):
    host = '127.0.0.1'
    sinks = 1
    sink_mode = 'framed'

    if not isinstance(ops, (list, tuple)):
        raise TypeError("ops must be a list or tuple of operations")

    # If no initial workers value is given, determine the minimum number
    # required at the start so that the cluster never goes below 1 worker.
    # If a number is given, then verify it is sufficient.
    if ops:
        if isinstance(ops[0], Recover):
            raise ValueError("The first operation cannot be Recover")
        lowest = lowest_point(ops)
        if lowest < 1:
            min_workers = abs(lowest) + 1
        else:
            min_workers = 1
        if isinstance(initial, int):
            logging.debug('initial: {}'.format(initial))
            logging.debug('min: {}'.format(min_workers))
            assert(initial >= min_workers)
            workers = initial
        else:
            workers = min_workers
    else:  # Test is only for setup using initial workers
        assert(initial > 0)
        workers = initial

    logging.info("Initial cluster size: {}".format(workers))

    partition_multiplier = 5  # Used in partition count creation
    # create the sequence generator and the reader
    msg = MultiSequenceGenerator(base_parts=workers * partition_multiplier - 1)

    # Start cluster
    logging.debug("Creating cluster")
    with Cluster(command=command, host=host, sources=sources,
                 workers=workers, sinks=sinks, sink_mode=sink_mode,
                 runner_data=runner_data) as cluster:

        # start senders
        for s in range(sources):
            sender = Sender(cluster.source_addrs[0],
                            Reader(msg),
                            batch_size=10,
                            interval=0.01,
                            reconnect=True)
            cluster.add_sender(sender, start=True)

        # let the senders send some data first
        time.sleep(1)

        # loop over ops, keeping the result and passing it to the next op
        res = None
        sender_paused = False
        assert(not cluster.get_crashed_workers())
        for op in ops:
            res_ops.append(op)
            logging.info("Executing: {}".format(op))
            res = op.apply(cluster, res)
            assert(not cluster.get_crashed_workers())

        # Wait a full second for things to calm down
        time.sleep(1)

        # Tell the multi-sequence-sender to stop
        msg.stop()

        # Validate all sender values caught up
        stop_value = max(msg.seqs)
        t0 = time.time()
        while True:
            try:
                assert(len(msg.seqs) == msg.seqs.count(stop_value))
                break
            except:
                if time.time() - t0 > 2:
                    logging.error("msg.seqs aren't all equal: {}"
                        .format(msg.seqs))
                    raise
            time.sleep(0.1)

        # Create await_values for the sink based on the stop values from the
        # multi sequence generator

        await_values = []
        for part, val in enumerate(msg.seqs):
            key = '{:07d}'.format(part)
            data = '[{},{},{},{}]'.format(*[val-x for x in range(3,-1,-1)])
            await_values.append((key, data))

        cluster.sink_await(values=await_values, func=parse_sink_value)
        logging.info("Completion condition achieved. Shutting down cluster.")

        # Use validator to validate the data in at-least-once mode
        # save sink data to a file
        if validate_output:
            # TODO: move to validations.py
            out_file = os.path.join(cluster.res_dir, 'received.txt')
            cluster.sinks[0].save(out_file)

            # Validate captured output
            logging.info("Validating output")
            cmd_validate = ('validator -i {out_file} -e {expect} -a'
                            .format(out_file = out_file,
                                    expect = stop_value))
            res = run_shell_cmd(cmd_validate)
            try:
                assert(res.success)
                logging.info("Validation successful")
            except:
                raise AssertionError('Validation failed with the following '
                                     'error:\n{}'.format(res.output))

        # Validate worker actually underwent recovery
        if cluster.restarted_workers:
            # TODO: move to validations.py
            logging.info("Validating recovery")
            pattern = "RESILIENCE\: Replayed \d+ entries from recovery log file\."
            for r in cluster.restarted_workers:
                stdout = r.get_output()
                try:
                    assert(re.search(pattern, stdout) is not None)
                    logging.info("{} recovered successfully".format(r.name))
                except AssertionError:
                    raise AssertionError('Worker does not appear to have performed '
                                         'recovery as expected. Worker output is '
                                         'included below.\nSTDOUT\n---\n%s'
                                         % stdout)