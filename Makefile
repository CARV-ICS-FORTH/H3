# Copyright [2019] [FORTH-ICS]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

REGISTRY_NAME?=carvicsforth
H3_VERSION=$(shell cat VERSION)

.PHONY: containers containers-push

containers:
	docker build -f Dockerfile --target h3 -t $(REGISTRY_NAME)/h3:$(H3_VERSION) .
	docker build -f Dockerfile --target h3-s3proxy -t $(REGISTRY_NAME)/h3-s3proxy:$(H3_VERSION) .
	docker build -f Dockerfile --build-arg BUILD_TYPE=Debug --target h3-builder -t $(REGISTRY_NAME)/h3:$(H3_VERSION)-dev .

containers-push:
	docker push $(REGISTRY_NAME)/h3:$(H3_VERSION)
	docker push $(REGISTRY_NAME)/h3-s3proxy:$(H3_VERSION)
	docker push $(REGISTRY_NAME)/h3:$(H3_VERSION)-dev
