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

export const read1 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor]);
}
export const read2 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1]);
}
export const read3 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2]);
}
export const read4 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3]);
}
export const read5 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4]);
}
export const read6 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5]);
}
export const read7 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6]);
}
export const read8 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7]);
}
export const read9 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7], buffer[cursor + 8]);
}
export const read10 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7], buffer[cursor + 8], buffer[cursor + 9]);
}
export const read11 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7], buffer[cursor + 8], buffer[cursor + 9], buffer[cursor + 10]);
}
export const read12 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7], buffer[cursor + 8], buffer[cursor + 9], buffer[cursor + 10], buffer[cursor + 11]);
}
export const read13 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7], buffer[cursor + 8], buffer[cursor + 9], buffer[cursor + 10], buffer[cursor + 11], buffer[cursor + 12]);
}
export const read14 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7], buffer[cursor + 8], buffer[cursor + 9], buffer[cursor + 10], buffer[cursor + 11], buffer[cursor + 12], buffer[cursor + 13]);
}
export const read15 = (buffer: Uint8Array, cursor: number) => {
    return String.fromCharCode(buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3], buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7], buffer[cursor + 8], buffer[cursor + 9], buffer[cursor + 10], buffer[cursor + 11], buffer[cursor + 12], buffer[cursor + 13], buffer[cursor + 14]);
}