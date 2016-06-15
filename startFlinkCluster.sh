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
flink_worker3="ls8ws010"
filename=$2

printf "\n\nUser: $username\nMaster: $flink_master\nWorker1: $flink_worker1\nWorker2: $flink_worker2\nWorker3: $flink_worker3\n\n"


if [[ $1 == "start" ]]; then

    echo "Start master..."
    master_start_cmd="cd flink; ./bin/start-cluster.sh;"
    start_master=`ssh "$username"@"$flink_master".cs.uni-dortmund.de "$master_start_cmd"`

    echo "Wait for master to start..."
    sleep 10

    workerstartcmd="cd flink; ./bin/taskmanager.sh start streaming;"

    echo "Start worker 1..."
    echo `ssh "$username"@"$flink_worker1".cs.uni-dortmund.de "$workerstartcmd"`

    echo "Start worker 2..."
    echo `ssh "$username"@"$flink_worker2".cs.uni-dortmund.de "$workerstartcmd"`

    echo "Start worker 3..."
    echo `ssh "$username"@"$flink_worker3".cs.uni-dortmund.de "$workerstartcmd"`
else

    echo "Stop master..."
    master_start_cmd="cd flink; ./bin/stop-cluster.sh;"
    start_master=`ssh "$username"@"$flink_master".cs.uni-dortmund.de "$master_start_cmd"`

    workerstopcmd="cd flink; ./bin/taskmanager.sh stop-all;"

    echo "Stop worker 1..."
    echo `ssh "$username"@"$flink_worker1".cs.uni-dortmund.de "$workerstopcmd"`

    echo "Stop worker 2..."
    echo `ssh "$username"@"$flink_worker2".cs.uni-dortmund.de "$workerstopcmd"`

    echo "Stop worker 3..."
    echo `ssh "$username"@"$flink_worker3".cs.uni-dortmund.de "$workerstopcmd"`
fi

