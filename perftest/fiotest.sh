#!/usr/bin/env bash

set -e

ROOT_PATH=$(cd $(dirname $BASH_SOURCE)/..; pwd)

OP=${1:-"usage"}; shift
TEST_BASE=${1:-"/mnt/cfs/perftest01"} ; shift
TEST_TIME=${1:-"$(date +%Y%m%d_%H%M%S)"} ; shift

LOG_DIR=${ROOT_PATH}/log/fiotest/${TEST_TIME}
REPORT_DIR=${ROOT_PATH}/report/fiotest
ERR_LOG=/tmp/fiotest-err.log
mkdir -p ${LOG_DIR} ${REPORT_DIR}

fio_direct=${FIO_DIRECT:-0}
fio_fallocate=${FIO_FALLOCATE:-0}
fio_iodepth=${FIO_IODEPTH:-4}
fio_ioengine=${FIO_IOENGINE:-"psync"}
fio_size=${FIO_SIZE:-"1G"}
fio_numjobs=${FIO_NUMJOBS:-20}
fio_runtime=${FIO_RUNTIME:-60}
mpi_nodes=${MPI_NODES:-8}
mpi_slots=${MPI_SLOTS:-4}

op_types="write read randwrite randread randrw"
bs_sizes="4M 1M 256k 64k 16k 4k"
mpi_process="1 4 16 64"
mpi_clients="1 2 4 8"

gen_report() {
  test_name=${1:?"need log in file"}
  in_file=${2:?"need log in file"}
  out_file=${3:?"need log out file"}
  iops=$(cat $in_file | grep "IOPS=" | awk '{print $2}' | tr 'k=,' 'K ' | awk '{print $2}' | numfmt --from=iec | awk '{ s+=$1 } END { print s } ')
  bw=$(cat $in_file | grep "IOPS=" | awk '{print $3}' | tr 'k=iB' 'K ' | awk '{print $2}' | numfmt --from=iec | awk '{ s+=$1 } END { print s } ')
  lat=$(cat $in_file | grep " lat (" | grep "stdev" | awk '{print $5}' | tr '=.' ' ' | awk '{print $2}' | awk 'BEGIN{ count=1 }{ s+=$1; count+=1 } END { print s/count } ')
  echo "${test_name}  $iops  $bw  $lat" >> $out_file
}

fio_test() {
    rw=${1:?"need op"}
    bs=${2:?"need bs"}

    host_ip=$(hostname -I | tr -d " ")
    test_id=${host_ip}-$(date +%N)-$RANDOM
    test_dir=${TEST_BASE}/fiotest/$(date +%Y%m%d_%H%M%S)/${test_id}
    mkdir -p $test_dir

    test_target="-directory=${test_dir}"
    if [ "$rw" == "randwrite" -o "$rw" == "randrw" ] ; then
      test_target="-filename=${test_dir}/$test_id"
      fio -rw=write $test_target -bs=${bs} -size=${fio_size} -group_reporting -ioengine=${fio_ioengine} -name="prewrite" -iodepth=${fio_iodepth} > /dev/null 2>&1
    fi
    test_name="${rw}-${bs}"
    log_file=${LOG_DIR}/${rw}_${bs}_${TEST_TIME}.tmp
    fio \
        ${test_target} \
        -name=${test_name} \
        -direct=${fio_direct} \
        -rw=${rw} \
        -bs=${bs} \
        -fallocate=${fio_fallocate} \
        -iodepth=${fio_iodepth} \
        -ioengine=${fio_ioengine} \
        -size=${fio_size} \
        -numjobs=${fio_numjobs} \
        -runtime=${fio_runtime} \
        -thread \
        -group_reporting \
        --output=${log_file} \
        2>$ERR_LOG

    cat ${log_file}
}

fio_batch() {
  for rw in $op_types ; do
    for bs in $bs_sizes ; do
      fio_test $rw $bs
    done
  done
}

mpi_run() {
  export PATH=$PATH:/usr/local/openmpi/bin/:/usr/local/ior/bin/
  export LD_LIBRARY_PATH=/usr/local/openmpi/lib:${LD_LIBRARY_PATH}
  export MPI_CC=mpicc

  process=${1:-"1"}
  client=${2:-"1"}
  op=${3:-"write"}
  bs=${4:-"16k"}

  np=$(($process * $client))

  mpi_args="-np $np --hostfile ${ROOT_PATH}/hosts/hosts${process}.txt"
  fiotest_cmd="sh ${BASH_SOURCE} op ${TEST_BASE} ${TEST_TIME} ${op} ${bs}"

  test_name=${process}_${client}_${op}_${bs}
  log_file=$LOG_DIR/${test_name}.log
  report_file=$REPORT_DIR/${TEST_TIME}.txt

  echo "mpirun $mpi_args $fiotest_cmd"
  mpirun \
    --allow-run-as-root \
    --prefix /usr/local/openmpi \
    --mca plm_rsh_agent rsh \
    --mca plm_rsh_force_rsh 1 \
    $mpi_args \
    $fiotest_cmd | tee -a ${log_file}

  gen_report ${test_name} ${log_file} ${report_file}
}

mpi_batch_run() {
  report_file=$REPORT_DIR/${TEST_TIME}.txt
  echo "process_client_op_bs iops bw lat" > $report_file

  for rw in $op_types ; do
    for bs in $bs_sizes ; do
      for process in $mpi_process; do
        for client in $mpi_clients; do
          mpi_run  $process $client $rw $bs
          sleep 10
        done
      done
    done
  done
}

usage() {
  cat <<EOF
$0 <cmd> <test_dir> [args]
cmd:
  op:       <op type>: write, read, rw, randwrite, randread
  batch:    batch run fio test
  mpi:      run fio test with mpi
  mpi_batch: batch run fio test with mpi
  gen_report: generate fio test report
EOF
}

case "$OP" in
  "op") fio_test "$@"  ;;
  "batch") fio_batch "$@"  ;;
  "mpi") mpi_run "$@"  ;;
  "mpi_batch") mpi_batch_run "$@"  ;;
  "gen_report") gen_report "$@"  ;;
  *)  usage ;;
esac

