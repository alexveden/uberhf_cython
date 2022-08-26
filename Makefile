PROJ_ROOT:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

p ?= $(PROJ_ROOT)
#GDB_EXECUTABLE:=/usr/local/bin/gdb13
GDB_EXECUTABLE:=gdb

CYTOOL:=cytool

# Python execution which should be used for building module, in debug mode
#   Typically original python is fine for debugging cython modules, but if you need more debug info (python symbols)
# 	you should build or install debug version of python
#
PY_EXEC:=python
#PY_EXEC:=python-dbg

TEST_EXEC:=pytest


.PHONY: build build-debug tests coverage annotate debug-file debug-tests debug-valgrind run clean

build-production:
	$(CYTOOL) -vvvv build

build:
	$(CYTOOL) build --debug --annotate

build-debug:
	$(CYTOOL) build --debug --annotate

tests: build-debug
	export PYTHONPATH=$(PROJ_ROOT):$(PYTHONPATH); python -m $(TEST_EXEC) --override-ini=cache_dir=$(PROJ_ROOT)/.cython_tools/.pytest_cache $(p)

tests-debug: build-debug
	$(CYTOOL) debug -t $(p)

run:  build-debug
	$(CYTOOL) run benchmarks/quotes_cache_raw.pyx@main

debug: build-debug
	$(CYTOOL) debug uberhf/datafeed/tests/mem_pool_benchmark.pyx@main



debug-file: build-debug
	#$(CYTOOL) run cy_tools_samples/debugging/abort.pyx@main
	#$(CYTOOL) debug cy_tools_samples/debugging/segfault.pyx@main
	#$(CYTOOL) debug uberhf/datafeed/tests/test_mem_pool_quotes.py --cygdb-verbosity=4
	$(CYTOOL) debug uberhf/datafeed/tests/test_mem_pool_quotes.py --cygdb-verbosity=4

lprun-file: build-debug
	$(CYTOOL) lprun cy_tools_samples/profiler/cy_module.pyx@approx_pi2"(10)" -m cy_tools_samples/profiler/cy_module.pyx

annotate-file: build-debug
	$(CYTOOL) annotate cy_tools_samples/low_level/dynamic_memory.pyx --browser

coverage: build-debug
	$(CYTOOL) cover . --browser

annotate: build-debug
	$(CYTOOL) annotate . --browser

clean:
	$(CYTOOL) clean -y -b