# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from enum import IntEnum

class CompatibleMode(IntEnum):
    SCHEMA_CONSISTENT = 1
    COMPATIBLE = 2

class MetaContext:
    __slots__ = (
        # Classes which has sent definitions to peer.
        "class_map",
        # Class definitions read from peer.
        "read_class_defs",
        "read_class_infos",
        # New class definition which needs sending to peer.
        "writing_class_defs"
    )
    def __init__(self):
        self.class_map = dict()
        self.read_class_defs = list()
        self.read_class_defs = list()
        self.writing_class_defs = list()

