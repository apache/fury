const Fury = require('../dist');
const utils = require('../test/util');
const fury = new Fury.default();
const hessian = require('hessian.js');
const Benchmark = require('benchmark');
const { sample1 } = require('./sample');

const hessianAb = hessian.encode(sample1, '2.0');

const options = {
    classCache: new Map()
}
const definition = utils.mockData2Definition(hessian.decode(hessianAb, '2.0', options), "foo.bar");
fury.registerSerializerByDefinition(definition, "foo.bar");
const furyAb = fury.marshal(hessian.decode(hessianAb, '2.0', options), 'foo.bar');

// console.time("start");
// console.log(process.pid)
// while (true) {
//     fury.unmarshal(ab);
// }

var suite = new Benchmark.Suite;

suite.add('fury', function () {
    fury.unmarshal(furyAb)
})
    .add('hessian', function () {
    hessian.decode(hessianAb, '2.0', options)

    })
    .on('cycle', function (event) {
        console.log(String(event.target));
    })
    .on('complete', function () {
        console.log('Fastest is ' + this.filter('fastest').map('name'));
    })
    .run({ 'async': true });

