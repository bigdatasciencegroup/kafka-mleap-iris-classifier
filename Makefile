
.PHONY: help
help: ## Shows this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: raw-data
raw-data: ## Download project raw data
raw-data: data/iris/iris.data

data:
	mkdir data
data/iris: data
	mkdir data/iris
data/iris/iris.data: data/iris
	curl -o data/iris/iris.data \
		https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
	
target:
	mkdir target

sbt-docker-build:
	docker build \
		--build-arg BASE_IMAGE_TAG="8u242-jdk-stretch" \
		--build-arg SBT_VERSION="1.3.5" \
		--build-arg SCALA_VERSION="2.12.10" \
		-t scala-sbt \
		github.com/hseeberger/scala-sbt.git#:debian

sbt-assembly: ## Compile and package Kafka Streaming Application
sbt-assembly: sbt-docker-build target
	docker run -it --rm -w /home/sbtuser \
		--mount type=bind,source="$(PWD)/src",target=/home/sbtuser/src \
		--mount type=bind,source="$(PWD)/build.sbt",target=/home/sbtuser/build.sbt \
		--mount type=bind,source="$(PWD)/project",target=/home/sbtuser/project \
		--mount type=bind,source="$(PWD)/target",target=/home/sbtuser/target \
		scala-sbt sbt assembly