import pyximport
import numpy as np
import cython
import sys

import faulthandler

faulthandler.enable()

sys.path.append('/home/ubertrader/cloud/code/uberminer')

pyximport.install(
        setup_args={"include_dirs": np.get_include(), "script_args" : ["--force"], },
        language_level=3
)

from uberhf.datafeed.tests.experiments.shared_mem_debug import main

if __name__ == '__main__':
    main(20)

