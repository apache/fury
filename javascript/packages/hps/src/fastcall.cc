/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "v8-fast-api-calls.h"
#include <nan.h>

void IsLatin1Slow(const v8::FunctionCallbackInfo<v8::Value> &info) {
  v8::Local<v8::String> input = info[0].As<v8::String>();
  info.GetReturnValue().Set(Nan::New(input->IsOneByte()));
}

bool IsLatin1Fast(v8::Local<v8::Value> receiver,
                  const v8::FastOneByteString &source) {
  return true;
}

static v8::CFunction fast_is_latin1(v8::CFunction::Make(IsLatin1Fast));

void StringCopySlow(const v8::FunctionCallbackInfo<v8::Value> &info) {
  v8::Isolate *isolate = info.GetIsolate();
  v8::Local<v8::String> source = info[0].As<v8::String>();
  char *type_array =
      (char *)info[1].As<v8::Uint8Array>()->Buffer()->GetBackingStore()->Data();
  int32_t offset =
      info[2]->Int32Value(info.GetIsolate()->GetCurrentContext()).FromJust();

  v8::String::Utf8Value source_value(isolate, source);

  for (size_t i = 0; i < source_value.length(); i++) {
    *(type_array + (offset++)) = *((*source_value) + i);
  }
  info.GetReturnValue().Set(Nan::New(100));
}

void StringCopyFast(v8::Local<v8::Value> receiver,
                    const v8::FastOneByteString &source,
                    const v8::FastApiTypedArray<u_int8_t> &ab,
                    u_int32_t offset) {
  uint8_t *ptr;
  ab.getStorageIfAligned(&ptr);
  int32_t i = 0;
  memcpy(ptr + offset, source.data, source.length);
}

static v8::CFunction string_copy_fast(v8::CFunction::Make(StringCopyFast));

v8::Local<v8::FunctionTemplate> FastFunction(v8::Isolate *isolate,
                                             v8::FunctionCallback callback,
                                             const v8::CFunction *c_function) {
  return v8::FunctionTemplate::New(
      isolate, callback, v8::Local<v8::Value>(), v8::Local<v8::Signature>(), 0,
      v8::ConstructorBehavior::kThrow, v8::SideEffectType::kHasSideEffect,
      c_function);
}

void Init(v8::Local<v8::Object> exports) {
  v8::Local<v8::Context> context = exports->GetCreationContextChecked();
  v8::Isolate *isolate = context->GetIsolate();
  exports->Set(context, Nan::New("isLatin1").ToLocalChecked(),
               FastFunction(isolate, IsLatin1Slow, &fast_is_latin1)
                   ->GetFunction(context)
                   .ToLocalChecked());
  exports->Set(context, Nan::New("stringCopy").ToLocalChecked(),
               FastFunction(isolate, StringCopySlow, &string_copy_fast)
                   ->GetFunction(context)
                   .ToLocalChecked());
}

NODE_MODULE(fury_util, Init)