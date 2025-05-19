/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer_pack.dart';

abstract base class IterableSerializer extends Serializer<Iterable> {

  const IterableSerializer(super.objType, super.writeRef);

  @override
  void write(ByteWriter bw, Iterable v, SerializerPack pack) {
    bw.writeVarUint32Small7(v.length);
    TypeSpecWrap? elemWrap = pack.typeWrapStack.peek?.param0;
    if (elemWrap == null){
      for (var o in v) {
        pack.furySer.xWriteRefNoSer(bw, o, pack);
      }
      return;
    }
    if (elemWrap.hasGenericsParam){
      pack.typeWrapStack.push(elemWrap);
    }
    if (!elemWrap.certainForSer){
      for (var o in v) {
        pack.furySer.xWriteRefNoSer(bw, o, pack);
      }
    }else {
      Serializer? ser = elemWrap.ser;
      if (ser == null){
        for (var o in v) {
          pack.furySer.xWriteRefNoSer(bw, o, pack);
        }
      }else{
        for (var o in v) {
          pack.furySer.xWriteRefWithSer(bw, ser, o, pack);
        }
      }
    }
    if (elemWrap.hasGenericsParam){
      pack.typeWrapStack.pop();
    }
  }
}