# Makefile for building FedSDM

.PHONY: help install bundle rebuild build run-example stop-example

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  install        to install all build dependencies"
	@echo "  bundle         to pack all dependencies for use in the tool"
	@echo "  build          to build a fresh Docker image; calls install and bundle as well"
	@echo "  rebuild        to build a Docker image without re-bundling the dependencies"
	@echo "  run-example    to build a fresh Docker image and run the example containers"
	@echo "  stop-example   to stop all example containers and remove their data"

install:
	python3 -m pip install -r requirements-dev.txt
	npm i

bundle:
	python3 bundle-assets.py

rebuild:
	docker build -f Dockerfile.alpine . -t fedsdm:latest

build: install bundle rebuild

run-example: build
	docker-compose -f example/docker-compose.yml up -d

stop-example:
	docker-compose -f example/docker-compose.yml down -v
