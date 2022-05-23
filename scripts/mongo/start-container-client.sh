#!/bin/bash
docker run -i -t --name mongo_cli --network=mongonet mongo:latest /bin/bash