cargo flamegraph --release --features rocksdb,bench --test goatkv_bench --   --directory ./bench_data   --threads 1   --engine both   populate --key-nums 100000 --batch-size 1000 --value-size 1024 --seq
perf script -i perf.data | inferno-collapse-perf | inferno-flamegraph > flamegraph.svg
