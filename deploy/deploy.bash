#!/bin/bash

PROJECT_ROOT=${PROJECT_ROOT:="/home/dmp/dmp-graph"}
NEO_HOME=${NEO_HOME:="/opt/neo4j-community-2.0.1"}
SHOULD_RESTART=$1


function ensureRoot {
  if [[ "x${BUILD_NUMBER}" == "x" && $EUID -ne 0 ]]; then
     echo "This script must be run as root" 1>&2
     exit 1
  fi
}


function cdProjectRoot {
  pushd ${PROJECT_ROOT}
}

function shutdownNeo4j {
  service neo4j stop
}

function makeProject {
  cdProjectRoot
  mvn -PRELEASE -DskipTests clean package
}


function copyProject {
  cdProjectRoot
  cp target/graph-1.0-jar.with-dependencies.jar ${NEO_HOME}/plugins/
}

function startupNeo4j {
  service neo4j start
  service neo4j status
}

function restartNeo4j {
  shutdownNeo4j
  startupNeo4j
}

ensureRoot

makeProject

copyProject

if [[ "x${SHOULD_RESTART}" == "xyes" ]]; then
  restartNeo4j
fi
