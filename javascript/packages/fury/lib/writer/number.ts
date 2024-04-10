const float32View = new Float32Array(1);
const int32View = new Int32Array(float32View.buffer);

export function toFloat16(value: number) {
  float32View[0] = value;
  const floatValue = int32View[0];
  const sign = (floatValue >>> 16) & 0x8000; // sign only
  const exponent = ((floatValue >>> 23) & 0xff) - 127; // extract exponent from floatValue
  const significand = floatValue & 0x7fffff; // extract significand from floatValue

  if (exponent === 128) { // floatValue is NaN or Infinity
    return sign | ((exponent === 128) ? 0x7c00 : 0x7fff);
  }

  if (exponent > 15) {
    return sign | 0x7c00; // return Infinity
  }

  if (exponent < -14) {
    return sign | 0x3ff; // returns ±max subnormal
  }

  if (exponent <= 0) {
    return sign | ((significand | 0x800000) >> (1 - exponent + 10));
  }

  return sign | ((exponent + 15) << 10) | (significand >> 13);
}
