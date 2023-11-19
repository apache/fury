# Fury JavaScript 

## Install
```shell
npm install @furyjs/fury
npm install @furyjs/hps
```

## Usage
```Javascript
import Fury, { Type, InternalSerializerType } from '@furyjs/fury';

/**
 * @furyjs/hps use v8's fast-calls-api that can be called directly by jit, ensure that the version of Node is 20 or above.
 * Experimental feature, installation success cannot be guaranteed at this moment
 * If you are unable to install the module, replace it with `const hps = null;`
 **/
import hps from '@furyjs/hps';

// Now we describe data structures using JSON, but in the future, we will use more ways.
const description = Type.object('example.foo', {
  foo: Type.string(),
});
const fury = new Fury({ hps });
const { serialize, deserialize } = fury.registerSerializer(description);
const input = serialize({ foo: 'hello fury' });
const result = deserialize(input);
console.log(result);
```

## Packages

### fury
Implement the protocol of fury, generate javascript code runtime, to make sure that all the code could be jit by v8 efficiently

### hps
Nodejs high-performance suite, ensuring that your Node.js version is 18 or later.
hps is use for detect the string type in v8. Fury support latin1 and utf8 string both, we should get the certain type of string before write it
in buffer. It is slow to detect the string is latin1 or utf8, but hps can detect it by a hack way, which is called FASTCALL in v8. 
so it is not stable now.

