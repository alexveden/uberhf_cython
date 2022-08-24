import zmq
import numpy as np
from Cython.Build import cythonize
from setuptools import setup
from setuptools.extension import Extension
import os
from Cython.Compiler import Options

project_extensions = [
    Extension("*",
              ["**/*.pyx"],
              # get rid of weird Numpy API warnings
              define_macros=[("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION")],
              # sudo ln -s /usr/lib/x86_64-linux-gnu/libzmq.so /usr/local/lib/libzmq.so
              libraries=["zmq"],
              #library_dirs=['/home/ubertrader/cloud/code/uberhf/.cython_tools/lib'],
              # Also may require: pip install pyzmq --install-option="--zmq=bundled"
              include_dirs=[os.path.dirname(__file__)] + [np.get_include()],
              #extra_link_args=[f"-Wl,-rpath=/home/ubertrader/cloud/code/uberhf/.cython_tools/lib"],
              ),
]
cythonize_kwargs = dict(
        include_path=[np.get_include()],
        # Skip cython language level warnings by default!
        language_level="3",
)

setup(name="uberhf",
      ext_modules=cythonize(project_extensions, **cythonize_kwargs),
      )