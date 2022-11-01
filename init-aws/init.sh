#!/usr/bin/env bash

set -eux

awslocal dynamodb create-table --cli-input-json file://\/docker-entrypoint-initaws.d/users-table.json --region ap-northeast-1
