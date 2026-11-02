SHELL := /bin/bash
ROOT := $(CURDIR)
GO ?= go
export GOCACHE ?= $(ROOT)/.cache/go-build
export GOMODCACHE ?= $(ROOT)/.cache/go-mod
export GOFLAGS ?= -buildvcs=false

.PHONY: all go-build worker-build worker-bazel test go-vet go-test worker-test python-test deps-up deps-down history
all: go-build worker-build
go-build:
	mkdir -p build/bin
	$(GO) build -trimpath -o build/bin/remex-scheduler ./scheduler/cmd/remex-scheduler
	$(GO) build -trimpath -o build/bin/remex ./cmd/remex
	$(GO) build -trimpath -o build/bin/remex-metrics ./cmd/remex-metrics
	cd checker && $(GO) build -trimpath -o ../build/bin/remex-checker ./cmd/remex-check
	cd checker && $(GO) build -trimpath -o ../build/bin/remex-record ./cmd/remex-record
	cd checker && $(GO) build -trimpath -o ../build/bin/remex-adapt ./cmd/remex-adapt
worker-bazel:
	.tools/bazel.official build //worker:remex-worker //worker:worker-tests //worker:remote-tests //worker:polling-tests //worker:nvml-tests
worker-build:
	cmake -S worker -B build/worker -DCMAKE_BUILD_TYPE=RelWithDebInfo
	cmake --build build/worker -j $${REMEX_BUILD_JOBS:-4}
test: go-test worker-test python-test
go-vet:
	$(GO) vet ./api/... ./cmd/... ./regress/... ./scheduler/... ./tests/...
	cd checker && $(GO) vet ./...
go-test:
	# Name the source roots: ./... would also walk build/, where benchmark runs leave large Bazel trees.
	$(GO) test -race ./api/... ./cmd/... ./regress/... ./scheduler/... ./tests/...
	cd checker && $(GO) test -race ./...
worker-test: worker-build
	ctest --test-dir build/worker --output-on-failure
python-test: go-build
	python3 -m unittest discover -s tests/regress -p 'test_*.py'
	python3 tests/protocol/checker_boundary_test.py
	python3 tests/protocol/checker_capture_boundary_test.py
	python3 tests/protocol/hardware_corpus_boundary_test.py
	python3 tests/protocol/regress_cancel_boundary_test.py
deps-up:
	docker compose -f deploy/compose.yaml up -d --wait postgres bazel-remote
deps-down:
	docker compose -f deploy/compose.yaml down
history:
	python3 scripts/generate-history.py --output build/planted-history
