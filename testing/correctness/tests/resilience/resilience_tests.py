from resilience import (Crash,
                        Grow,
                        Recover,
                        Shrink,
                        Wait,
                        _test_resilience)


# TODO:
# - [ ] Figure out issue with sender unable to connect
# - [ ] Add test code generator
# - [ ] add: AddSource operation (and logic to validate)
# - [ ] background crash detector with notifier-short-circuit capability
#       to eliminate timeout periods when a test fails due to crashed workers
# - [ ] Clean out dev debug logs

#############
# Test spec(s)
#############

# A fixed test:
def test_grow1_shrink1_crash2_wait1_recover2():
    command = 'multi_partition_detector --depth 1'
    ops = [Grow(1), Shrink(1), Crash(2), Wait(1), Recover(2)]
    _test_resilience(command, ops, initial=None, validate_output=True)

