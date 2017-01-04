#!/usr/bin/env bash
git reset --hard
git pull
sbt clean assembly
spark-submit --verbose \
	--class myOrg.Example \
	--master local[*] \
	--driver-memory=30G \
	--conf spark.default.parallelism=36 \
	target/scala-2.11/sparkMiniSample-assembly-0.1-SNAPSHOT.jar
