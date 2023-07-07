import Fury from '../index';
import { genReadSerializer } from '../lib/codeGen';
import {describe, expect, test} from '@jest/globals';

describe('map', () => {
  test('should map work', () => {
    const fury = new Fury();
    const input = fury.marshal(new Map([["foo", "bar"], ["foo2", "bar2"]]));
    const result = fury.unmarshal(
        input
    );
    expect(result).toEqual(new Map([["foo", "bar"],["foo2", "bar2"]]))
  });
});


