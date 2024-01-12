#!/bin/bash

#
# Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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
#

exec java \
 -Xms64m -Xmx256m \
 -Dlog4j.configurationFile=/log/log4j2.xml \
 --module-path "/opt/creek/service/lib" \
 --module creek.kafka.test.service.inbuilt/org.creekservice.internal.kafka.test.service.inbuilt.ServiceMain