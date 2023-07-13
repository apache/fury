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

