#!/bin/bash

exec java -javaagent:opentelemetry-javaagent.jar -jar s3-tech-adapter.jar "$@"
