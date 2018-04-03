#!/bin/sh

benchmarks/run-benchmarks.sh && make benchmarks-slow && \
benchmarks/parse.sh && benchmarks/plot-histogram.gp
