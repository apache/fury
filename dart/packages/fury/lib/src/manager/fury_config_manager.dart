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

import 'package:fury/src/config/fury_config.dart';

class FuryConfigManager{
  // singleton
  static final FuryConfigManager _instance = FuryConfigManager._();
  static FuryConfigManager get inst => _instance;
  FuryConfigManager._();

  int configId = 0;
  int get nextConfigId => configId++;

  FuryConfig createConfig({
    bool isLittleEndian = true,
    bool refTracking = true,
    bool basicTypesRefIgnored = true,
    bool timeRefIgnored = true,
    // bool stringRefIgnored = true,
  }) {
    return FuryConfig.onlyForManager(
      nextConfigId,
      isLittleEndian: isLittleEndian,
      refTracking: refTracking,
      basicTypesRefIgnored: basicTypesRefIgnored,
      timeRefIgnored: timeRefIgnored,
      // stringRefIgnored: stringRefIgnored,
    );
  }
}
