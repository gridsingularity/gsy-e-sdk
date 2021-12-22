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
