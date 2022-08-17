import numpy
from Cython.Build import cythonize
from setuptools import setup
from setuptools.extension import Extension

import zmq

extensions = [
    Extension(
        "zmq_cython",
        ["zmq_cython.pyx"],
        #libraries=["libzmq"],
        #library_dirs=zmq.get_library_dirs(),
        #library_dirs=["/home/ubertrader/.local/share/virtualenvs/cython_tools-V-pB7gNR/lib/python3.9/site-packages/zmq"],
        include_dirs=zmq.get_includes() + [numpy.get_include()],
    )
]
setup(name="cython-zmq-example", ext_modules=cythonize(extensions))