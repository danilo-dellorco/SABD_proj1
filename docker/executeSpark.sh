#! /bin/bash
docker-compose exec spark-master /bin/sh -c "./bin/spark-submit --class basics.SquareEvenNumbers --master spark://spark-master:7077 hdfs://hdfs-master:54310/handson-spark-1.0.jar"