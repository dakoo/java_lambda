# Makefile for building and deploying a Gradle-based AWS Lambda

# Variables (override these by specifying on the command line, e.g. `make deploy LAMBDA_FUNCTION_NAME=myFunc`)
LAMBDA_FUNCTION_NAME ?= common_data_service_poc
REGION ?= ap-northeast-2
JAR_NAME ?= java-lambda-kafka-dynamodb.jar
PROFILE ?= dev

# Default target
.PHONY: all
all: build

############################################################
## Build Targets
############################################################

.PHONY: build
build:
	# Use the Gradle wrapper (if present) or gradle if installed
	./gradlew clean shadowJar

.PHONY: clean
clean:
	./gradlew clean

############################################################
## Deploy Targets
############################################################

.PHONY: deploy
deploy: build
	# After building, deploy to AWS Lambda
	@echo "Deploying to Lambda function: $(LAMBDA_FUNCTION_NAME) in region $(REGION)"
	aws --profile $(PROFILE) lambda update-function-code \
		--function-name $(LAMBDA_FUNCTION_NAME) \
		--region $(REGION) \
		--zip-file fileb://build/libs/$(JAR_NAME)
