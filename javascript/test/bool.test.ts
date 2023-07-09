
import Fury, { TypeDescription, InternalSerializerType } from '@furyjs/fury';
import {describe, expect, test} from '@jest/globals';

describe('bool', () => {
  test('should false work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const input = fury.marshal(false);
    const result = fury.unmarshal(
        input
    );
    expect(result).toEqual(false)
  });
  test('should true work', () => {
    const hps = process.env.enableHps ? require('@furyjs/hps') : null;
    const fury = new Fury({ hps });    
    const input = fury.marshal(true);
    const result = fury.unmarshal(
        input
    );
    expect(result).toEqual(true)
  });
});


