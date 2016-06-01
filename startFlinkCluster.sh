#!/usr/bin/env bash


if [ $# != 1 ]
	then
	echo "An argument is required: start or stop."
	exit -1
fi

username="egorov"
flink_master="ls8ws007"
flink_worker1="ls8ws008"
flink_worker2="ls8ws009"
filename=$2

printf "\n\nUser: $username\nMaster: $flink_master\nWorker1: $flink_worker1\nWorker2: $flink_worker2\n\n"


if [[ $1 == "start" ]]; then

    echo "Start master..."
    master_start_cmd="cd flink1; ./bin/start-cluster-streaming.sh;"
    start_master=`ssh "$username"@"$flink_master".cs.uni-dortmund.de "$master_start_cmd"`

    echo "Wait for master to start..."
    sleep 10

    workerstartcmd="cd flink1; ./bin/taskmanager.sh start streaming;"

    echo "Start worker 1..."
    start_worker1=`ssh "$username"@"$flink_worker1".cs.uni-dortmund.de "$workerstartcmd"`

    echo "Start worker 2..."
    start_worker2=`ssh "$username"@"$flink_worker2".cs.uni-dortmund.de "$workerstartcmd"`
else

    echo "Stop master..."
    master_start_cmd="cd flink; ./bin/stop-cluster.sh;"
    start_master=`ssh "$username"@"$flink_master".cs.uni-dortmund.de "$master_start_cmd"`

    workerstopcmd="cd flink1; ./bin/taskmanager.sh stop-all;"

    echo "Stop worker 1..."
    start_master=`ssh "$username"@"$flink_worker1".cs.uni-dortmund.de "$workerstopcmd"`

    echo "Stop worker 2..."
    start_master=`ssh "$username"@"$flink_worker2".cs.uni-dortmund.de "$workerstopcmd"`
fi

