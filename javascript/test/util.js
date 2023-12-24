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

const { InternalSerializerType } = require('@furyjs/fury')

const mockData2Description = (data, tag) => {
    if (data === null || data === undefined) {
        return null;
    }
    if (Array.isArray(data)) {
        const item = mockData2Description(data[0], tag);
        if (!item) {
            throw new Error('empty array can\'t convert')
        }
        return {
            type: InternalSerializerType.ARRAY,
            label: 'array',
            options: {
                inner: item,
            }
        }
    }
    if (data instanceof Date) {
        return {
            type: InternalSerializerType.TIMESTAMP,
            label: 'timestamp'
        }
    }
    if (typeof data === 'string') {
        return {
            type: InternalSerializerType.STRING,
            label: "string",
        }
    }
    if (data instanceof Set) {
        return {
            type: InternalSerializerType.FURY_SET,
            label: "set",
            options: {
                key: mockData2Description([...data.values()][0], tag),
            }
        }
    }
    if (data instanceof Map) {
        return {
            type: InternalSerializerType.MAP,
            label: "map",
            options: {
                key: mockData2Description([...data.keys()][0], tag),
                value: mockData2Description([...data.values()][0], tag),
            }
        }
    }
    if (typeof data === 'boolean') {
        return {
            type: InternalSerializerType.BOOL,
            label: "boolean",
        }
    }
    if (typeof data === 'number') {
        return {
            type: InternalSerializerType.INT32,
            label: "int32"
        }
    }
    if (typeof data === 'object') {
        return {
            type: InternalSerializerType.FURY_TYPE_TAG,
            label: "object",
            options: {
                props: Object.fromEntries(Object.entries(data).map(([key, value]) => {
                    return [key, mockData2Description(value, `${tag}.${key}`)]
                }).filter(([k, v]) => Boolean(v))),
                tag
            }

        }
    }
    throw `unkonw data type ${typeof data}`
}
module.exports.mockData2Description = mockData2Description;

