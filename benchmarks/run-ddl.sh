#!/bin/bash
flag=$1
output=""
exe=""
benchmark=tpcc_org
sf=50
threads=31
duration=10
node_memory_gb=30
cdc_threads=5
scan_threads=3
enable_cdc_schema_lock=0
enable_ddl_keys=0
pcommit_queue_length=500000
enable_late_scan_join=0
ddl_total=1
enable_parallel_scan_cdc=1
print_interval_ms=1000
cdc_physical_workers_only=1
scan_physical_workers_only=1
client_load_per_core=4500
latency_stat_interval_ms=100
enable_lazy_on_conflict_do_nothing=1
ddl_start_time=2
ddl_example=0
benchmark_paras=""
oddlb_table_size=100000000
oddlb_workload="D"

if [ "$flag" == "00" ]; then
  output=ddl-logs/tpcc-add-column-tesseract-copy.log
  exe=./corobase_DDL_COPY
elif [ "$flag" == "01" ]; then
  output=ddl-logs/tpcc-add-column-tesseract-no-copy.log
  exe=./corobase_DDL_COPY
  ddl_example=5
elif [ "$flag" == "02" ]; then
  output=ddl-logs/tpcc-add-column-tesseract-opt-lazy.log
  exe=./corobase_DDL_OPT_LAZY_COPY
elif [ "$flag" == "03" ]; then
  output=ddl-logs/tpcc-add-column-lazy.log
  exe=./corobase_DDL_LAZY_COPY
  enable_ddl_keys=1
elif [ "$flag" == "04" ]; then
  output=ddl-logs/tpcc-add-column-block.log
  exe=./corobase_DDL_BLOCK
elif [ "$flag" == "05" ]; then
  output=ddl-logs/tpcc-add-column-tesseract-copy-hyperthread.log
  exe=./corobase_DDL_COPY
  cdc_physical_workers_only=0
  scan_physical_workers_only=0
elif [ "$flag" == "10" ]; then
  output=ddl-logs/tpcc-table-split-tesseract-copy.log
  exe=./corobase_DDL_COPY
  ddl_example=1
  sf=200
  node_memory_gb=40
elif [ "$flag" == "11" ]; then
  output=ddl-logs/tpcc-table-split-tesseract-no-copy.log
  exe=./corobase_DDL_COPY
  ddl_example=6
  sf=200
  node_memory_gb=40
elif [ "$flag" == "12" ]; then
  output=ddl-logs/tpcc-table-split-tesseract-opt-lazy.log
  exe=./corobase_DDL_OPT_LAZY_COPY
  ddl_example=1
  sf=200
  node_memory_gb=40
elif [ "$flag" == "13" ]; then
  output=ddl-logs/tpcc-table-split-lazy.log
  exe=./corobase_DDL_LAZY_COPY
  enable_ddl_keys=1
  ddl_example=1
  sf=200
  node_memory_gb=40
elif [ "$flag" == "14" ]; then
  output=ddl-logs/tpcc-table-split-block.log
  exe=./corobase_DDL_BLOCK
  ddl_example=1
  sf=200
  node_memory_gb=40
elif [ "$flag" == "15" ]; then
  output=ddl-logs/tpcc-table-split-tesseract-copy-hyperthread.log
  exe=./corobase_DDL_COPY
  cdc_physical_workers_only=0
  scan_physical_workers_only=0
  ddl_example=1
  sf=200
  node_memory_gb=40
elif [ "$flag" == "20" ]; then
  output=ddl-logs/tpcc-preaggregation-tesseract-copy.log
  exe=./corobase_DDL_COPY
  enable_ddl_keys=1
  ddl_example=2
  cdc_threads=3
  scan_threads=5
elif [ "$flag" == "21" ]; then
  output=ddl-logs/tpcc-preaggregation-tesseract-no-copy.log
  exe=./corobase_DDL_COPY
  enable_ddl_keys=1
  ddl_example=7
  cdc_threads=3
  scan_threads=5
elif [ "$flag" == "22" ]; then
  output=ddl-logs/tpcc-preaggregation-tesseract-opt-lazy.log
  exe=./corobase_DDL_OPT_LAZY_COPY
  enable_ddl_keys=1
  ddl_example=2
  cdc_threads=3
  scan_threads=5
elif [ "$flag" == "23" ]; then
  output=ddl-logs/tpcc-preaggregation-lazy.log
  exe=./corobase_DDL_LAZY_COPY
  enable_ddl_keys=1
  ddl_example=2
  cdc_threads=3
  scan_threads=5
elif [ "$flag" == "24" ]; then
  output=ddl-logs/tpcc-preaggregation-block.log
  exe=./corobase_DDL_BLOCK
  enable_ddl_keys=1
  ddl_example=2
  cdc_threads=3
  scan_threads=5
elif [ "$flag" == "30" ]; then
  output=ddl-logs/tpcc-table-join-tesseract-copy.log
  exe=./corobase_DDL_COPY
  enable_ddl_keys=1
  ddl_example=4
elif [ "$flag" == "31" ]; then
  output=ddl-logs/tpcc-table-join-tesseract-no-copy.log
  exe=./corobase_DDL_COPY
  enable_ddl_keys=1
  ddl_example=8
elif [ "$flag" == "32" ]; then
  output=ddl-logs/tpcc-table-join-tesseract-opt-lazy.log
  exe=./corobase_DDL_OPT_LAZY_COPY
  enable_ddl_keys=1
  ddl_example=4
elif [ "$flag" == "33" ]; then
  output=ddl-logs/tpcc-table-join-lazy.log
  exe=./corobase_DDL_LAZY_COPY
  enable_ddl_keys=1
  ddl_example=4
elif [ "$flag" == "34" ]; then
  output=ddl-logs/tpcc-table-join-block.log
  exe=./corobase_DDL_BLOCK
  enable_ddl_keys=1
  ddl_example=4
elif [ "$flag" == "40" ]; then
  output=ddl-logs/tpcc-index-creation-tesseract-copy.log
  exe=./corobase_DDL_COPY
  enable_ddl_keys=1
  ddl_example=3
elif [ "$flag" == "41" ]; then
  output=ddl-logs/tpcc-index-creation-block.log
  exe=./corobase_DDL_BLOCK
  enable_ddl_keys=1
  ddl_example=3
elif [ "$flag" == "50" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-copy.log
  exe=./corobase_DDL_COPY
  benchmark="oddl"
  duration=20
  node_memory_gb=45
elif [ "$flag" == "51" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-no-copy.log
  exe=./corobase_DDL_COPY
  ddl_example=1
  benchmark="oddl"
  duration=20
  node_memory_gb=45
elif [ "$flag" == "52" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-opt-lazy.log
  exe=./corobase_DDL_OPT_LAZY_COPY
  benchmark="oddl"
  duration=20
  node_memory_gb=45
elif [ "$flag" == "53" ]; then
  output=ddl-logs/oddlb-add-column-lazy.log
  exe=./corobase_DDL_LAZY_COPY
  enable_ddl_keys=1
  benchmark="oddl"
  duration=20
  node_memory_gb=45
elif [ "$flag" == "54" ]; then
  output=ddl-logs/oddlb-add-column-block.log
  exe=./corobase_DDL_BLOCK
  benchmark="oddl"
  duration=20
  node_memory_gb=45
elif [ "$flag" == "55" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-copy-8r2w.log
  exe=./corobase_DDL_COPY
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  oddlb_workload="B"
elif [ "$flag" == "56" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-no-copy-8r2w.log
  exe=./corobase_DDL_COPY
  ddl_example=1
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  oddlb_workload="B"
elif [ "$flag" == "57" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-opt-lazy-8r2w.log
  exe=./corobase_DDL_OPT_LAZY_COPY
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  oddlb_workload="B"
elif [ "$flag" == "58" ]; then
  output=ddl-logs/oddlb-add-column-lazy-8r2w.log
  exe=./corobase_DDL_LAZY_COPY
  enable_ddl_keys=1
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  oddlb_workload="B"
elif [ "$flag" == "59" ]; then
  output=ddl-logs/oddlb-add-column-block-8r2w.log
  exe=./corobase_DDL_BLOCK
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  oddlb_workload="B"
elif [ "$flag" == "60" ]; then
  output=ddl-logs/oddlb-verification-tesseract-copy.log
  exe=./corobase_DDL_COPY
  benchmark="oddl"
  ddl_example=2
elif [ "$flag" == "61" ]; then
  output=ddl-logs/oddlb-verification-block.log
  exe=./corobase_DDL_BLOCK
  benchmark="oddl"
  ddl_example=2
elif [ "$flag" == "70" ]; then
  output=ddl-logs/oddlb-add-column-and-verification-tesseract-copy.log
  exe=./corobase_DDL_COPY
  benchmark="oddl"
  ddl_example=3
  duration=20
  node_memory_gb=45
elif [ "$flag" == "71" ]; then
  output=ddl-logs/oddlb-add-column-and-verification-block.log
  exe=./corobase_DDL_BLOCK
  benchmark="oddl"
  ddl_example=3
  duration=20
  node_memory_gb=45
elif [ "$flag" == "80" ]; then
  output=ddl-logs/tpcc-credit-check-preaggregation-tesseract-copy.log
  exe=./corobase_DDL_COPY
  enable_ddl_keys=1
  ddl_example=2
  benchmark="tpcc+"
elif [ "$flag" == "81" ]; then
  output=ddl-logs/tpcc-credit-check-preaggregation-tesseract-no-copy.log
  exe=./corobase_DDL_COPY
  enable_ddl_keys=1
  ddl_example=7
  benchmark="tpcc+"
elif [ "$flag" == "82" ]; then
  output=ddl-logs/tpcc-credit-check-preaggregation-tesseract-opt-lazy.log
  exe=./corobase_DDL_OPT_LAZY_COPY
  enable_ddl_keys=1
  ddl_example=2
  benchmark="tpcc+"
elif [ "$flag" == "83" ]; then
  output=ddl-logs/tpcc-credit-check-preaggregation-lazy.log
  exe=./corobase_DDL_LAZY_COPY
  enable_ddl_keys=1
  ddl_example=2
  benchmark="tpcc+"
elif [ "$flag" == "84" ]; then
  output=ddl-logs/tpcc-credit-check-preaggregation-block.log
  exe=./corobase_DDL_BLOCK
  enable_ddl_keys=1
  ddl_example=2
  benchmark="tpcc+"
elif [ "$flag" == "90" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-copy-10M.log
  exe=./corobase_DDL_COPY
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  oddlb_table_size=10000000
elif [ "$flag" == "91" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-copy-50M.log
  exe=./corobase_DDL_COPY
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  oddlb_table_size=50000000
elif [ "$flag" == "100" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-copy-3C5S.log
  exe=./corobase_DDL_COPY
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  scan_threads=5
  cdc_threads=3
elif [ "$flag" == "101" ]; then
  output=ddl-logs/oddlb-add-column-tesseract-copy-4C4S.log
  exe=./corobase_DDL_COPY
  benchmark="oddl"
  duration=20
  node_memory_gb=45
  scan_threads=4
  cdc_threads=4
elif [ "$flag" == "110" ]; then
  output=ddl-logs/tpcc-add-constraint-tesseract-copy.log
  exe=./corobase_DDL_COPY
  ddl_example=9
  enable_ddl_keys=1
elif [ "$flag" == "111" ]; then
  output=ddl-logs/tpcc-add-constraint-block.log
  exe=./corobase_DDL_BLOCK
  ddl_example=9
  enable_ddl_keys=1
else
  exit;
fi

if [[ "$benchmark" == "tpcc_org" || "$benchmark" == "tpcc+" ]]; then
  benchmark_paras="-d $ddl_start_time -e $ddl_example"
elif [ "$benchmark" == "oddl" ]; then
  benchmark_paras="-d $ddl_start_time -e $ddl_example -s $oddlb_table_size -w $oddlb_workload"
  client_load_per_core=8000
fi

rm -f $output

for((i = 0; i < 3; i++))
do
  echo "round: $i" >> $output
  date >> $output
  echo "./run.sh $exe $benchmark $sf $threads $duration \"-node_memory_gb=$node_memory_gb -cdc_threads=$cdc_threads -scan_threads=$scan_threads -enable_cdc_verification_test=0 -enable_cdc_schema_lock=$enable_cdc_schema_lock -enable_ddl_keys=$enable_ddl_keys -pcommit_queue_length=$pcommit_queue_length -enable_late_scan_join=$enable_late_scan_join -enable_lazy_background=1 -ddl_total=$ddl_total -enable_parallel_scan_cdc=$enable_parallel_scan_cdc -print_interval_ms=$print_interval_ms -no_copy_verification_version_add=1 -cdc_physical_workers_only=$cdc_physical_workers_only -scan_physical_workers_only=$scan_physical_workers_only -client_load_per_core=$client_load_per_core -latency_stat_interval_ms=$latency_stat_interval_ms -enable_lazy_on_conflict_do_nothing=$enable_lazy_on_conflict_do_nothing\" \"$benchmark_paras\"" | tee -a $output

  hog-machine.sh ./run.sh $exe $benchmark $sf $threads $duration "-node_memory_gb=$node_memory_gb -cdc_threads=$cdc_threads -scan_threads=$scan_threads -enable_cdc_verification_test=0 -enable_cdc_schema_lock=$enable_cdc_schema_lock -enable_ddl_keys=$enable_ddl_keys -pcommit_queue_length=$pcommit_queue_length -enable_late_scan_join=$enable_late_scan_join -enable_lazy_background=1 -ddl_total=$ddl_total -enable_parallel_scan_cdc=$enable_parallel_scan_cdc -print_interval_ms=$print_interval_ms -no_copy_verification_version_add=1 -cdc_physical_workers_only=$cdc_physical_workers_only -scan_physical_workers_only=$scan_physical_workers_only -client_load_per_core=$client_load_per_core -latency_stat_interval_ms=$latency_stat_interval_ms -enable_lazy_on_conflict_do_nothing=$enable_lazy_on_conflict_do_nothing" "$benchmark_paras" 2>&1 | tee -a $output

  echo >> $output
  echo >> $output
done
exit;
