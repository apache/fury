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

package org.apache.fury.resolver;

class ClassNameBytes {
  private final long packageHash;
  private final long classNameHash;

  ClassNameBytes(long packageHash, long classNameHash) {
    this.packageHash = packageHash;
    this.classNameHash = classNameHash;
  }

  @Override
  public boolean equals(Object o) {
    // ClassNameBytes is used internally, skip
    ClassNameBytes that = (ClassNameBytes) o;
    return packageHash == that.packageHash && classNameHash == that.classNameHash;
  }

  @Override
  public int hashCode() {
    int result = 31 + (int) (packageHash ^ (packageHash >>> 32));
    result = result * 31 + (int) (classNameHash ^ (classNameHash >>> 32));
    return result;
  }
}
