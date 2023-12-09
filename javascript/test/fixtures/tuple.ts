import { Type } from "../../packages/fury";

export const tupleType1 = Type.tuple( [
  Type.object('example.foo.1',{
    a: Type.object('example.foo.1.1',{
      b: Type.string()
    })
  }),
  Type.object('example.foo.2',{
    a: Type.object('example.foo.2.1',{
      c: Type.string()
    })
  })
]);

export const tupleType2 = Type.tuple( [
  Type.object('example.foo.1',{
    a: Type.object('example.foo.1.1',{
      b: Type.string()
    })
  }),
  Type.object('example.bar.1',{
    a: Type.object('example.bar.1.1',{
      b: Type.string()
    })
  }),
  Type.object('example.bar.2',{
    a: Type.object('example.bar.2.1',{
      c: Type.string()
    })
  })
]);

export const tupleType3 = Type.tuple([
  Type.string(),
  Type.bool(),
  Type.uint32(),
  Type.tuple([
    Type.binary()
  ])
])

export const tupleObjectTag = 'tuple-object-wrapper';

export const tupleObjectDescription =  Type.object(tupleObjectTag, {
  tuple1: tupleType1,
  tuple1_: tupleType1,
  tuple2: tupleType2,
  tuple2_: tupleType2,
});

export const tupleObjectType3Tag = 'tuple-object-type3-tag';
export const tupleObjectType3Description = Type.object(tupleObjectType3Tag, {
  tuple: tupleType3
})
