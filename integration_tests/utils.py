import subprocess
from time import sleep


def wait_for_log_to_appear_in_container_logs(container_name: str, log_string: str) -> bool:
    """Wait for string to appear in docker container log."""
    counter = 1
    while counter < 10:
        result = subprocess.check_output(f"docker logs {container_name}",
                                         shell=True, text=True, stderr=subprocess.STDOUT)
        if log_string in result:
            return True
        counter += 1
        sleep(3)
    return False


def wait_for_active_aggregator(context, time_out: int = 10, poll_frequency_s: int = 1):
    """Wait for active aggregator in the behave context."""
    counter = 1
    while counter < time_out:
        if context.aggregator.is_active:
            return
        counter += 1
        sleep(poll_frequency_s)
    assert False, "Aggregator was not set to active in the time_out period."
