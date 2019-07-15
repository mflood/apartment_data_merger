#!/bin/sh
#docker run --name mail_forward -p 8000:8000 --rm mail_forward:latest
docker run --name digible --network="host" --env-file=docker.env --rm -i --log-driver=none -a stdout -a stderr digible:latest
