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

import static io.fury.type.TypeUtils.getSizeOfPrimitiveType;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import io.fury.util.ReflectionUtils;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * A utility class to group class fields into groups.
 * <li>primitive fields
 * <li>boxed primitive fields
 * <li>final fields
 * <li>collection fields
 * <li>map fields
 * <li>other fields
 *
 * @author chaokunyang
 */
@SuppressWarnings("UnstableApiUsage")
public class DescriptorGrouper {
  // sort primitive descriptors from largest to smallest, if size is the same,
  // sort by field name to fix order.
  public static final Comparator<Descriptor> PRIMITIVE_COMPARATOR =
      (d1, d2) -> {
        int c =
            getSizeOfPrimitiveType(Primitives.unwrap(d2.getRawType()))
                - getSizeOfPrimitiveType(Primitives.unwrap(d1.getRawType()));
        if (c == 0) {
          c = DescriptorGrouper.COMPARATOR_BY_TYPE_AND_NAME.compare(d1, d2);
        }
        return c;
      };

  private static final Set<Class<?>> COMPRESS_TYPES =
      ImmutableSet.of(int.class, Integer.class, long.class, Long.class);

  // sort primitive descriptors from largest to smallest but let compress fields ends
  // in tail. if size is the same, sort by field name to fix order.
  public static final Comparator<Descriptor> PRIMITIVE_COMPRESSED_COMPARATOR =
      (d1, d2) -> {
        Class<?> t1 = Primitives.unwrap(d1.getRawType());
        Class<?> t2 = Primitives.unwrap(d2.getRawType());
        boolean t1Compress = COMPRESS_TYPES.contains(t1);
        boolean t2Compress = COMPRESS_TYPES.contains(t2);
        if ((t1Compress && t2Compress) || (!t1Compress && !t2Compress)) {
          int c = getSizeOfPrimitiveType(t2) - getSizeOfPrimitiveType(t1);
          if (c == 0) {
            c = DescriptorGrouper.COMPARATOR_BY_TYPE_AND_NAME.compare(d1, d2);
          }
          return c;
        }
        if (t1Compress) {
          return 1;
        }
        // t2 compress
        return -1;
      };

  /** Comparator based on field type, name and declaring class. */
  public static final Comparator<Descriptor> COMPARATOR_BY_TYPE_AND_NAME =
      (d1, d2) -> {
        // sort by type so that we can hit class info cache more possibly.
        // sort by field name to fix order if type is same.
        int c =
            d1
                // Use raw type instead of generic type so that fields with type token
                // constructed in ClassDef which take pojo as non-final Object type
                // will have consistent order between processes if the fields doesn't exist in peer.
                .getRawType()
                .getName()
                .compareTo(d2.getRawType().getName());
        if (c == 0) {
          c = d1.getName().compareTo(d2.getName());
          if (c == 0) {
            // Field name duplicate in super/child classes.
            c = d1.getDeclaringClass().compareTo(d2.getDeclaringClass());
          }
        }
        return c;
      };

  private final Collection<Descriptor> primitiveDescriptors;
  private final Collection<Descriptor> boxedDescriptors;
  // The element type should be final.
  private final Collection<Descriptor> collectionDescriptors;
  // The key/value type should be final.
  private final Collection<Descriptor> mapDescriptors;
  private final Collection<Descriptor> finalDescriptors;
  private final Collection<Descriptor> otherDescriptors;

  /**
   * Create a descriptor grouper.
   *
   * @param descriptors descriptors may have field with same name.
   * @param descriptorUpdator create a new descriptor from original one.
   * @param primitiveComparator comparator for primitive/boxed fields.
   * @param comparator comparator for non-primitive fields.
   */
  public DescriptorGrouper(
      Collection<Descriptor> descriptors,
      boolean descriptorsGroupedOrdered,
      Function<Descriptor, Descriptor> descriptorUpdator,
      Comparator<Descriptor> primitiveComparator,
      Comparator<Descriptor> comparator) {
    this.primitiveDescriptors =
        descriptorsGroupedOrdered ? new ArrayList<>() : new TreeSet<>(primitiveComparator);
    this.boxedDescriptors =
        descriptorsGroupedOrdered ? new ArrayList<>() : new TreeSet<>(primitiveComparator);
    this.collectionDescriptors =
        descriptorsGroupedOrdered ? new ArrayList<>() : new TreeSet<>(comparator);
    this.mapDescriptors = descriptorsGroupedOrdered ? new ArrayList<>() : new TreeSet<>(comparator);
    this.finalDescriptors =
        descriptorsGroupedOrdered ? new ArrayList<>() : new TreeSet<>(comparator);
    this.otherDescriptors =
        descriptorsGroupedOrdered ? new ArrayList<>() : new TreeSet<>(comparator);
    for (Descriptor descriptor : descriptors) {
      if (descriptor.getRawType().isPrimitive()) {
        primitiveDescriptors.add(descriptorUpdator.apply(descriptor));
      } else if (TypeUtils.isBoxed(descriptor.getRawType())) {
        boxedDescriptors.add(descriptorUpdator.apply(descriptor));
      } else if (TypeUtils.isCollection(descriptor.getRawType())) {
        collectionDescriptors.add(descriptorUpdator.apply(descriptor));
      } else if (TypeUtils.isMap(descriptor.getRawType())) {
        mapDescriptors.add(descriptorUpdator.apply(descriptor));
      } else if (Modifier.isFinal(descriptor.getRawType().getModifiers())) {
        finalDescriptors.add(descriptorUpdator.apply(descriptor));
      } else {
        otherDescriptors.add(descriptorUpdator.apply(descriptor));
      }
    }
  }

  public Collection<Descriptor> getPrimitiveDescriptors() {
    return primitiveDescriptors;
  }

  public Collection<Descriptor> getBoxedDescriptors() {
    return boxedDescriptors;
  }

  public Collection<Descriptor> getCollectionDescriptors() {
    return collectionDescriptors;
  }

  public Collection<Descriptor> getMapDescriptors() {
    return mapDescriptors;
  }

  public Collection<Descriptor> getFinalDescriptors() {
    return finalDescriptors;
  }

  public Collection<Descriptor> getOtherDescriptors() {
    return otherDescriptors;
  }

  private static Descriptor createDescriptor(Descriptor d) {
    if (!Modifier.isPublic(d.getRawType().getModifiers())) {
      // Non-public class can't be accessed from generated code.
      // (Ignore protected/package level access for simplicity.
      // Since class members whose type are non-public class are rare,
      // it doesn't have much impact on performance.)
      TypeToken<?> publicSuperType = ReflectionUtils.getPublicSuperType(d.getTypeToken());
      return d.copy(publicSuperType, null, null);
    } else {
      // getter/setter may lose some inner state of an object, so we set them to null.
      if (d.getReadMethod() == null && d.getWriteMethod() == null) {
        return d;
      }
      return d.copy(d.getTypeToken(), null, null);
    }
  }

  public static DescriptorGrouper createDescriptorGrouper(
      Collection<Descriptor> descriptors,
      boolean descriptorsGroupedOrdered,
      boolean compressNumber) {
    Comparator<Descriptor> comparator =
        compressNumber ? PRIMITIVE_COMPRESSED_COMPARATOR : PRIMITIVE_COMPARATOR;
    return new DescriptorGrouper(
        descriptors,
        descriptorsGroupedOrdered,
        DescriptorGrouper::createDescriptor,
        comparator,
        COMPARATOR_BY_TYPE_AND_NAME);
  }
}
