import Fury from '../index';
import {describe, expect, test} from '@jest/globals';

describe('string', () => {
  test('should string work', () => {
    const fury = new Fury();
    const input = fury.marshal("123")
    const result = fury.unmarshal(
        input
    );
    expect(result).toEqual("123")
  });
});


