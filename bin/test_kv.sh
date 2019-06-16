#!/bin/bash

SCRIPT_HOME=$(cd "$(dirname "$0")"; pwd)

if [[ -z ${JAVA_HOME} ]]; then
    echo "JAVA_HOME env not provided"
fi

JAVA=${JAVA_HOME}/bin/java

CLASSPATH=${SCRIPT_HOME}/../target/index-1.0-SNAPSHOT.jar

JAVA_OPTS=" -XX:PermSize=256m -XX:MaxPermSize=256m -Xms4196m -Xmx4196m "
JAVA_OPTS+="-XX:+UseParNewGC -XX:NewRatio=4 -XX:+CMSClassUnloadingEnabled "
JAVA_OPTS+="-XX:MinHeapFreeRatio=100 -XX:MaxHeapFreeRatio=100 "
JAVA_OPTS+="-XX:CMSMaxAbortablePrecleanTime=1000 -XX:+ExplicitGCInvokesConcurrent "
JAVA_OPTS+="-XX:MaxTenuringThreshold=4 -XX:TargetSurvivorRatio=8 -XX:+HeapDumpOnOutOfMemoryError "

${JAVA} ${JAVA_OPTS} -cp ${CLASSPATH} io.wzp.index.Main $@