#!/usr/bin/env bash

# default values for variables
process_path="examples/alexey-queues.xml"
run=false
build=false
nimbus="localhost"
stormmainclass="storm.deploy"

# handle the argument options
function USAGE {
    echo ""
    echo "Usage:"
    echo "-p    <path to XML process file>      if not defined default value will be used"
    echo "-n    <nimbus host ip adress>         localhost as default"
    echo "-m    <storm.deploy|storm.run>        define mainclass to deploy or run local cluster"
    echo "-r                                    run the deployment process"
    echo "-b                                    build new packages"
    exit 1
}

while getopts ":p:n:m: :rb" optname
  do
    case "$optname" in
      "p") process_path=$OPTARG;;
      "r") run=true;;
      "b") build=true;;
      "n") nimbus=$OPTARG;;
      "m") stormmainclass=$OPTARG;;
      "?") USAGE;;
      ":")
        echo "No argument value for option $OPTARG"
        USAGE
        ;;
      *)
        echo "Unknown error while processing options"
        USAGE
        ;;
    esac
  done

echo ""
echo "Using following process as topology: $process_path"
echo "Rebuild: $build"
echo "Running: $run"
echo "Nimbus host: $nimbus"
echo "Storm mainclass: $stormmainclass"

# stop and print the usage if both r and b were not set
# or if mainclass is set to sth different then flink.{deploy,run}
if ( ( [ ${run} == "false" ] ) && ( [ ${build} == "false" ] ) ) || \
        ( [[ ! ${stormmainclass} =~ ^flink\.(deploy|run)$ ]] ) ; then
    USAGE
fi

# rebuild (package) the needed jars
if ${build}; then
    # package for deployment
    mvn -P deploy,standalone package

    # package for local start
    mvn -Dstorm.mainclass=${stormmainclass} -P standalone,!deploy package
fi

# run the deployment process
if ${run}; then
    # start the deployment
    java -jar \
        -Dnimbus.host=${nimbus} \
        -Dstorm.jar=target/streams-storm-0.9.22-SNAPSHOT-storm-provided.jar \
        target/streams-storm-0.9.22-SNAPSHOT-storm-compiled.jar \
        ${process_path}
fi
