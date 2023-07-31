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

package io.fury.type;

import io.fury.annotation.Internal;

/**
 * Stub class for object type which is final.
 *
 * <p>{@link Object} class will be used if isn't final. No {@link io.fury.resolver.ClassInfo} should
 * be created for this class since it has no fields, and doesn't have consistent class structure as
 * real class.
 *
 * @author chaokunyang
 */
@Internal
public final class FinalObjectTypeStub {}
