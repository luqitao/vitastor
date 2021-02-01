#!/bin/bash

if [ ! "$BASH_VERSION" ] ; then
    echo "Use bash to run this script ($0)" 1>&2
    exit 1
fi

format_error()
{
    echo $(echo -n -e "\033[1;31m")$1$(echo -n -e "\033[m")
    $ETCDCTL get --prefix /vitastor > ./testdata/etcd-dump.txt
    exit 1
}
format_green()
{
    echo $(echo -n -e "\033[1;32m")$1$(echo -n -e "\033[m")
}

set -e -x

trap 'kill -9 $(jobs -p)' EXIT

ETCD=${ETCD:-etcd}
ETCD_PORT=${ETCD_PORT:-12379}

rm -rf ./testdata
mkdir -p ./testdata
dd if=/dev/zero of=./testdata/test_osd1.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd2.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd3.bin bs=1024 count=1 seek=$((1024*1024-1))

$ETCD -name etcd_test --data-dir ./testdata/etcd \
    --advertise-client-urls http://127.0.0.1:$ETCD_PORT --listen-client-urls http://127.0.0.1:$ETCD_PORT \
    --initial-advertise-peer-urls http://127.0.0.1:$((ETCD_PORT+1)) --listen-peer-urls http://127.0.0.1:$((ETCD_PORT+1)) \
    --max-txn-ops=100000 --auto-compaction-retention=10 --auto-compaction-mode=revision &>./testdata/etcd.log &
ETCD_PID=$!
ETCD_URL=127.0.0.1:$ETCD_PORT/v3
ETCDCTL="${ETCD}ctl --endpoints=http://$ETCD_URL"

./osd --osd_num 1 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd1.bin 2>/dev/null) &>./testdata/osd1.log &
OSD1_PID=$!
./osd --osd_num 2 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd2.bin 2>/dev/null) &>./testdata/osd2.log &
OSD2_PID=$!
./osd --osd_num 3 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd3.bin 2>/dev/null) &>./testdata/osd3.log &
OSD3_PID=$!

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url http://$ETCD_URL --etcd_prefix "/vitastor" &>./testdata/mon.log &
MON_PID=$!

$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"xor","pg_size":3,"pg_minsize":2,"parity_chunks":1,"pg_count":1,"failure_domain":"osd"}}'

sleep 2

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(. | length) != 0 and (.[0].items["1"]["1"].osd_set | sort) == ["1","2","3"]'); then
    format_error "FAILED: 1 PG NOT CONFIGURED"
fi

if ! ($ETCDCTL get /vitastor/pg/state/1/1 --print-value-only | jq -s -e '(. | length) != 0 and .[0].state == ["active"]'); then
    format_error "FAILED: 1 PG NOT UP"
fi

echo leak:fio >> testdata/lsan-suppress.txt
echo leak:tcmalloc >> testdata/lsan-suppress.txt
#LSAN_OPTIONS=suppressions=`pwd`/testdata/lsan-suppress.txt LD_PRELOAD=libasan.so.5 \
#    fio -thread -name=test -ioengine=./libfio_sec_osd.so -bs=4k -fsync=128 `$ETCDCTL get /vitastor/osd/state/1 --print-value-only | jq -r '"-host="+.addresses[0]+" -port="+(.port|tostring)'` -rw=write -size=32M

LSAN_OPTIONS=suppressions=`pwd`/testdata/lsan-suppress.txt LD_PRELOAD=libasan.so.5 \
    fio -thread -name=test -ioengine=./libfio_cluster.so -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=1G -cluster_log_level=10

format_green OK