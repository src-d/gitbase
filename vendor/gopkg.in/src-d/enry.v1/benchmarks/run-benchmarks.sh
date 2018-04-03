#!/bin/sh

mkdir -p benchmarks/output &&  go test -run NONE -bench=. -benchtime=120s -timeout=100h >benchmarks/output/enry_total.bench && \
benchmarks/linguist-total.rb 5 >benchmarks/output/linguist_total.bench
