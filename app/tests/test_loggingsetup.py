import logging
import digible.loggingsetup as loggingsetup

def test_global_init():
    loggingsetup.init(loglevel=logging.DEBUG)

