const obj = {
    $class: 'foo.bar',
    $: {
        hello: "aabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefbcdef",
        world: "JSON是由美国程序员道格拉斯·克罗克福特构想和设计的一种轻量级资料交换格式。其内容由属性和值所组成，因此也有易于阅读和处理的优势。JSON是独立于编程语言的资料格式，其不仅是JavaScript的子集，也采用了C语言家族的习惯用法，目前也有许多编程语言都能够将其解析和字符串化，其广泛使用的程度也使其成为通用的资料格式。",
        c: null,
        t: true,
        s1: false,
        b: 12321312,
        s: 32323
    }
}
const data = {
    $class: 'foo.bar2',
    $: {
        a: obj,
        b: obj,
        c: obj,
        d: obj,
        f: obj,
        s: [
            obj,
            obj,
            obj,
        ]
    }
};
module.exports.sample1 = data;