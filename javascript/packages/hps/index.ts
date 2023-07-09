const hps: Hps = require('bindings')('hps.node');

interface Hps {
    isLatin1: (str: string) => boolean
    stringCopy: (str: string, dist: Uint8Array, offset: number) => void
}

const { isLatin1, stringCopy } = hps;

export {
    isLatin1,
    stringCopy
}