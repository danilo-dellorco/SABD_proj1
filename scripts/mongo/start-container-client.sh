#!/bin/bash
docker run --rm -t -i --network=mongonet --name=mongo_client mongo:latest /bin/bash
