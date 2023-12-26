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

import { fromUint8Array } from '../packages/fury/lib/platformBuffer';
import { BinaryReader } from '../packages/fury/lib/reader';
import { Config, RefFlags } from '../packages/fury/lib/type';
import { BinaryWriter } from '../packages/fury/lib/writer';
import { describe, expect, test } from '@jest/globals';


function num2Bin(num: number) {
    return (num >>> 0).toString(2);
}

[
    {
        useLatin1: true,
        useSliceString: true,
    },
    {
        useLatin1: false,
    }
].forEach((config: Config) => {
    describe('writer', () => {
        test('should unsigned int work', () => {
            const writer = BinaryWriter(config);
            let len = 0;
            [
                8,
                16,
                32,
                64
            ].forEach((x, y) => {
                {
                    writer[`uint${x}`](10);
                    len += x / 8;
                    var ab = writer.dump();
                    expect(ab.byteLength).toBe(len);
                    if (x === 64) {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getBigUint${x}`](writer.getCursor() - x / 8, true)).toBe(BigInt(10));
                    } else {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getUint${x}`](writer.getCursor() - x / 8, true)).toBe(10);
                    }
                    expect(writer.getCursor()).toBe(len);
                }

                {
                    writer[`uint${x}`](-1);
                    len += x / 8;
                    var ab = writer.dump();
                    expect(ab.byteLength).toBe(len);
                    if (x === 64) {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getBigUint${x}`](writer.getCursor() - x / 8, true)).toBe(BigInt(2 ** x) - BigInt(1));
                    } else {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getUint${x}`](writer.getCursor() - x / 8, true)).toBe(2 ** x - 1);
                    }
                    expect(writer.getCursor()).toBe(len);
                }

                {
                    writer[`uint${x}`](2 ** x);
                    len += x / 8;
                    var ab = writer.dump();
                    expect(ab.byteLength).toBe(len);
                    if (x === 64) {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getBigUint${x}`](writer.getCursor() - x / 8, true)).toBe(BigInt(0));
                    } else {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getUint${x}`](writer.getCursor() - x / 8, true)).toBe(0);
                    }
                    expect(writer.getCursor()).toBe(len);
                }
            })


        });

        test('should int work', () => {
            const writer = BinaryWriter(config);
            let len = 0;
            [
                8,
                16,
                32,
                64
            ].forEach((x, y) => {
                {
                    writer[`int${x}`](10);
                    len += x / 8;
                    var ab = writer.dump();
                    expect(ab.byteLength).toBe(len);
                    if (x === 64) {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getBigInt${x}`](writer.getCursor() - x / 8, true)).toBe(BigInt(10));
                    } else {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getInt${x}`](writer.getCursor() - x / 8, true)).toBe(10);
                    }
                    expect(writer.getCursor()).toBe(len);
                }

                {
                    writer[`int${x}`](2 ** x);
                    len += x / 8;
                    var ab = writer.dump();
                    expect(ab.byteLength).toBe(len);
                    if (x === 64) {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getBigInt${x}`](writer.getCursor() - x / 8, true)).toBe(BigInt(0));
                    } else {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getInt${x}`](writer.getCursor() - x / 8, true)).toBe(0);
                    }
                    expect(writer.getCursor()).toBe(len);
                }

                {
                    writer[`int${x}`](-1);
                    len += x / 8;
                    var ab = writer.dump();
                    expect(ab.byteLength).toBe(len);
                    if (x === 64) {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getBigInt${x}`](writer.getCursor() - x / 8, true)).toBe(BigInt(-1));
                    } else {
                        expect(new DataView(ab.buffer, ab.byteOffset)[`getInt${x}`](writer.getCursor() - x / 8, true)).toBe(-1);
                    }
                    expect(writer.getCursor()).toBe(len);
                }
            })
        });

        test('should varint32 work', () => {
            [
                1,
                2,
                3,
                4,
            ].forEach(x => {
                {
                    const writer = BinaryWriter(config);
                    const value = (2 ** (x * 7)) - 1;
                    writer.varInt32(value);
                    const ab = writer.dump();
                    expect(ab.byteLength).toBe(x);
                    for (let index = 0; index < ab.byteLength - 1; index++) {
                        expect(num2Bin(ab[index])).toBe('11111111');
                    }
                    expect(num2Bin(ab[ab.byteLength - 1])).toBe('1111111');
                    const reader = BinaryReader(config);
                    reader.reset(ab);
                    const vari32 = reader.varInt32();
                    expect(vari32).toBe(value);
                }
                {
                    const writer = BinaryWriter(config);
                    const value = (2 ** (x * 7));
                    writer.varInt32(value);
                    const ab = writer.dump();
                    expect(ab.byteLength).toBe(x + 1);
                    for (let index = 0; index < ab.byteLength - 1; index++) {
                        expect(num2Bin(ab[index])).toBe('10000000');
                    }
                    expect(num2Bin(ab[ab.byteLength - 1])).toBe('1');
                    const reader = BinaryReader(config);
                    reader.reset(ab);
                    const vari32 = reader.varInt32();
                    expect(vari32).toBe(value);
                }
            });
        });

        test('should short latin1 string work', () => {
            const writer = BinaryWriter(config);
            writer.stringOfVarInt32("hello world");
            const ab = writer.dump();
            const reader = BinaryReader(config);
            reader.reset(ab);
            if (config.useLatin1) {
                expect(reader.uint8()).toBe(0);
            }
            const len = reader.varInt32();
            expect(len).toBe(11);
            const str = reader.stringLatin1(11);
            expect(str).toBe("hello world");
        });

        test('should long latin1 string work', () => {
            const writer = BinaryWriter(config);
            const str = new Array(10).fill('hello world').join('');
            writer.stringOfVarInt32(str);
            const ab = writer.dump();
            const reader = BinaryReader(config);
            reader.reset(ab);
            if (config.useLatin1) {
                expect(reader.uint8()).toBe(0);
            }
            const len = reader.varInt32();
            expect(len).toBe(110);
            expect(reader.stringLatin1(len)).toBe(str);
        });

        test('should short utf8 string work', () => {
            const writer = BinaryWriter(config);
            const str = new Array(1).fill('hello 你好 😁').join('');
            writer.stringOfVarInt32(str);
            const ab = writer.dump();
            const reader = BinaryReader(config);

            {
                reader.reset(ab);
                if (config.useLatin1) {
                    expect(reader.uint8()).toBe(1);
                }
                const len = reader.varInt32();
                expect(len).toBe(17);
                expect(reader.stringUtf8(len)).toBe(str);
            }
            {
                reader.reset(ab);
                expect(reader.stringOfVarInt32()).toBe(str);
            }
        });

        test('should long utf8 string work', () => {
            const writer = BinaryWriter(config);
            const str = new Array(10).fill('hello 你好 😁').join('');
            writer.stringOfVarInt32(str);
            const ab = writer.dump();
            const reader = BinaryReader(config);
            {
                reader.reset(ab);
                if (config.useLatin1) {
                    expect(reader.uint8()).toBe(1);
                }
                const len = reader.varInt32();
                expect(len).toBe(170);
                expect(reader.stringUtf8(len)).toBe(str);
            }
            {
                reader.reset(ab);
                expect(reader.stringOfVarInt32()).toBe(str);
            }
        });

        test('should buffer work', () => {
            const writer = BinaryWriter(config);
            writer.buffer(new Uint8Array([1, 2, 3, 4, 5]));
            const ab = writer.dump();
            const reader = BinaryReader(config);
            reader.reset(ab);
            expect(ab.byteLength).toBe(5);
            expect(ab[0]).toBe(1);
            expect(ab[1]).toBe(2);
            expect(ab[2]).toBe(3);
            expect(ab[3]).toBe(4);
            expect(ab[4]).toBe(5);
        });

        test('should bufferWithoutMemCheck work', () => {
            const writer = BinaryWriter(config);
            writer.bufferWithoutMemCheck(fromUint8Array(new Uint8Array([1, 2, 3, 4, 5])), 5);
            const ab = writer.dump();
            const reader = BinaryReader(config);
            reader.reset(ab);
            expect(ab.byteLength).toBe(5);
            expect(ab[0]).toBe(1);
            expect(ab[1]).toBe(2);
            expect(ab[2]).toBe(3);
            expect(ab[3]).toBe(4);
            expect(ab[4]).toBe(5);
        });

        test('should setUint32Position work', () => {
            const writer = BinaryWriter(config);
            writer.skip(10);
            writer.setUint32Position(0, 100);
            writer.setUint32Position(5, 100);
            const ab = writer.dump();
            expect(ab.byteLength).toBe(10);
            expect(ab[0]).toBe(100);
            expect(ab[5]).toBe(100);
        });

        test('should float work', () => {
            const writer = BinaryWriter(config);
            writer.float(10.01);
            const ab = writer.dump();
            expect(ab.byteLength).toBe(4);
            const reader = BinaryReader(config);
            reader.reset(ab);
            expect(reader.float().toFixed(2)).toBe((10.01).toFixed(2));
        });

        test('should double work', () => {
            const writer = BinaryWriter(config);
            writer.double(10.01);
            const ab = writer.dump();
            expect(ab.byteLength).toBe(8);
            const reader = BinaryReader(config);
            reader.reset(ab);
            expect(reader.double().toFixed(2)).toBe((10.01).toFixed(2));
        });

        test('should reserve work', () => {
            const writer = BinaryWriter(config);
            const byteLength = writer.getByteLen();
            const cursor = writer.getCursor();
            const reserved = writer.getReserved();
            writer.reserve(10);

            expect(writer.getReserved()).toBe(reserved + 10);
            expect(writer.getByteLen()).toBe(byteLength);
            expect(writer.getCursor()).toBe(cursor);
        });

        test('should reserve work', () => {
            const writer = BinaryWriter(config);
            const byteLength = writer.getByteLen();
            const cursor = writer.getCursor();
            const reserved = writer.getReserved();
            writer.reserve(1024 * 101);

            expect(writer.getReserved()).toBe(reserved + 1024 * 101);
            expect(writer.getByteLen()).toBe(byteLength * 2 + 1024 * 101);
            expect(writer.getCursor()).toBe(cursor);
        });

        test('should reset work', () => {
            const writer = BinaryWriter(config);
            writer.int16(100);
            writer.reset();
            expect(writer.getCursor()).toBe(0);
            expect(writer.getReserved()).toBe(0);
        });

        test('should int24 work', () => {
            const writer = BinaryWriter(config);
            writer.int24( (20 << 8)  | 10);
            const ab = writer.dump();
            const reader = BinaryReader({});
            reader.reset(ab)
            expect(reader.int8()).toBe(10);
            expect(reader.int16()).toBe(20);
        });
    });
})
