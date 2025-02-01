.PHONY: test bench profile clean benchmark pprof

# Default target
all: test

# Run all tests
test:
	go test -v ./...

# Run basic benchmarks
bench:
	@echo "Running benchmarks with auto-tuning..."
	@go test -v -bench=BenchmarkAll -benchmem | tee bench.txt

# Run comprehensive benchmarks with all sizes
benchmark:
	@echo "Running all benchmarks..."
	@echo "\nStandard operations benchmark:"
	go test -bench=BenchmarkOperations -benchmem
	@echo "\nProfile benchmark:"
	go test -bench=BenchmarkProfile -benchmem

# CPU profiling
cpu-profile:
	go test -cpuprofile=cpu.prof -bench=BenchmarkProfile
	go tool pprof cpu.prof

# Memory profiling
mem-profile:
	go test -memprofile=mem.prof -bench=BenchmarkProfile
	go tool pprof mem.prof

# Trace profiling
trace-profile:
	go test -trace=trace.out -bench=BenchmarkProfile
	go tool trace trace.out

# Clean up profiling files
clean:
	rm -f *.prof *.out

# Show all profiles
pprof: cpu-profile mem-profile trace-profile

# Help target
help:
	@echo "Available targets:"
	@echo "  test          - Run all tests"
	@echo "  bench         - Run basic benchmarks"
	@echo "  benchmark     - Run comprehensive benchmarks"
	@echo "  cpu-profile   - Run CPU profiling"
	@echo "  mem-profile   - Run memory profiling"
	@echo "  trace-profile - Run execution trace"
	@echo "  pprof         - Run all profiles"
	@echo "  clean         - Remove profiling files" 