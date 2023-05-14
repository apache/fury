
import Fury from '../index';
import {describe, expect, test} from '@jest/globals';

describe('bool', () => {
  test('should false work', () => {
    const fury = new Fury();
    const input = fury.marshal(false);
    const result = fury.unmarshal(
        new Uint8Array(input)
    );
    expect(result).toEqual(false)
  });
  test('should true work', () => {
    const fury = new Fury();
    const input = fury.marshal(true);
    const result = fury.unmarshal(
        new Uint8Array(input)
    );
    expect(result).toEqual(true)
  });
});


