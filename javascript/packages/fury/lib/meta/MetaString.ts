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

export class MetaString {
  // Encode function that handles all Unicode characters
  static encode(str: string): Uint8Array {
    let binaryString = '';
    for (let i = 0; i < str.length; i++) {
        binaryString += String.fromCharCode(str.charCodeAt(i));
    }
    const base64 = btoa(binaryString);
    const outputArray = new Uint8Array(base64.length);
    for (let i = 0; i < base64.length; i++) {
        outputArray[i] = base64.charCodeAt(i);
    }
    return outputArray;
  }

  // Decoding function that handles all Unicode characters
  static decode(bytes: Uint8Array): string {
    let base64 = '';
    for (let i = 0; i < bytes.length; i++) {
        base64 += String.fromCharCode(bytes[i]);
    }
    const binaryString = atob(base64);
    let outputString = '';
    for (let i = 0; i < binaryString.length; i++) {
        outputString += String.fromCharCode(binaryString.charCodeAt(i));
    }
    return outputString;
  }
}
