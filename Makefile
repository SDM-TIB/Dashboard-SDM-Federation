# Makefile for building FedSDM

.PHONY: help install bundle build run-example stop-example

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  install        to install all build dependencies"
	@echo "  bundle         to pack all dependencies for use in the tool"
	@echo "  build          to build a fresh Docker image; calls install and bundle as well"
	@echo "  run-example    to build a fresh Docker image and run the example containers"
	@echo "  stop-example   to stop all example containers and remove their data"

install:
	python3 -m pip install -r requirements.txt
	npm i

bundle:
	python3 bundle-assets.py

build: install bundle
	docker build -f Dockerfile.alpine . -t fedsdm:latest

run-example: build
	docker-compose -f example/docker-compose.yml up -d

stop-example:
	docker-compose -f example/docker-compose.yml down -v
