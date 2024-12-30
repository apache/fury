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

from pyfury import Buffer
from pyfury._serialization import MetaStringResolver, MetaStringBytes
from pyfury.meta.metastring import MetaStringEncoder


def test_metastring_resolver():
    resolver = MetaStringResolver()
    encoder = MetaStringEncoder("$", "_")
    metastr1 = encoder.encode("hello, world")
    metabytes1 = resolver.get_metastr_bytes(metastr1)
    buffer = Buffer.allocate(32)
    resolver.write_meta_string_bytes(buffer, metabytes1)
    assert resolver.read_meta_string_bytes(buffer) == metabytes1
    metabytes2 = MetaStringBytes(
        data=b"\xbf\x05\xa4q\xa9\x92S\x96\xa6IOr\x9ch)\x80",
        hashcode=-5456063526933366015,
    )
    resolver.write_meta_string_bytes(buffer, metabytes2)
    assert resolver.read_meta_string_bytes(buffer) == metabytes2
