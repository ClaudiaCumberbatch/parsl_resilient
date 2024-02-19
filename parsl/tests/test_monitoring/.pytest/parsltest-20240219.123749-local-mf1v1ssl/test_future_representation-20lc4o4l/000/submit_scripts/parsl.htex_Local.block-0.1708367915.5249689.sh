
export JOBNAME=$parsl.htex_Local.block-0.1708367915.5249689
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
PARSL_MONITORING_HUB_URL=udp://localhost:55055 PARSL_MONITORING_RADIO_MODE=htex PARSL_RUN_ID=ff092b64-fc32-46a9-b4f3-444265891fad PARSL_RUN_DIR=/Users/zhousicheng/Desktop/resilient/parsl_resilient/parsl/tests/test_monitoring/.pytest/parsltest-current/test_future_representation-20lc4o4l/000 process_worker_pool.py --debug  -a 127.0.0.1 -p 0 -c 1 -m None --poll 100 --task_port=54165 --result_port=54729 --cert_dir /Users/zhousicheng/Desktop/resilient/parsl_resilient/parsl/tests/test_monitoring/.pytest/parsltest-current/test_future_representation-20lc4o4l/000/htex_Local/certificates --logdir=/Users/zhousicheng/Desktop/resilient/parsl_resilient/parsl/tests/test_monitoring/.pytest/parsltest-current/test_future_representation-20lc4o4l/000/htex_Local --block_id=0 --hb_period=2  --hb_threshold=5 --cpu-affinity none --available-accelerators --uid 1112bafabf24 --monitor_resources --monitoring_url udp://localhost:55055 --run_id ff092b64-fc32-46a9-b4f3-444265891fad --radio_mode htex --sleep_dur 1 
}
for COUNT in $(seq 1 1 $WORKERCOUNT); do
    [[ "1" == "1" ]] && echo "Launching worker: $COUNT"
    CMD $COUNT &
    PIDS="$PIDS $!"
done

ALLFAILED=1
ANYFAILED=0
for PID in $PIDS ; do
    wait $PID
    if [ "$?" != "0" ]; then
        ANYFAILED=1
    else
        ALLFAILED=0
    fi
done

[[ "1" == "1" ]] && echo "All workers done"
if [ "$FAILONANY" == "1" ]; then
    exit $ANYFAILED
else
    exit $ALLFAILED
fi
