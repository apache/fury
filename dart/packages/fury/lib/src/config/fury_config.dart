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

import 'package:fury/src/config/config.dart';

class FuryConfig extends Config{
  final int  _configId;
  //final bool _isLittleEndian;
  final bool _refTracking;
  final bool _basicTypesRefIgnored;
  final bool _timeRefIgnored;
  final bool _stringRefIgnored;

  FuryConfig.onlyForManager(
    this._configId, {
    bool isLittleEndian = true,
    bool refTracking = true,
    bool basicTypesRefIgnored = true,
    bool timeRefIgnored = true,
    // bool stringRefIgnored = true,
  })
  : _refTracking = refTracking,
    _basicTypesRefIgnored = basicTypesRefIgnored,
    _timeRefIgnored = timeRefIgnored,
    _stringRefIgnored = false
  {
    // some checking works
    // assert(_xlangMode == true, 'currently only support xlang mode');
    //assert(_isLittleEndian == true, 'Non-Little-Endian format detected. Only Little-Endian is supported.');
  }

  //getters
  //bool get isLittleEndian => _isLittleEndian;
  bool get refTracking => _refTracking;
  int get configId => _configId;
  bool get basicTypesRefIgnored => _basicTypesRefIgnored;
  bool get timeRefIgnored => _timeRefIgnored;
  bool get stringRefIgnored => _stringRefIgnored;
}