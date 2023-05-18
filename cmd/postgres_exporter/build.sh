#!/bin/bash
export GOROOT=/d/sdk/golang1.19/go
export GOPATH=/c/project
export PATH=$GOROOT/bin:$PATH

export GOPROXY="https://goproxy.cn"



CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build  -o postgres_exporter .