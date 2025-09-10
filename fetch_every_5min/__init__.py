import logging
from . import runner

def main(sf_timer) -> None:
    try:
        runner.run_one_cycle()   # make sure your run.py has this function
    except Exception as e:
        logging.exception("Runner failed: %s", e)
