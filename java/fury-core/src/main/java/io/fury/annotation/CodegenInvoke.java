package io.fury.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation to mark a method will be invoked by generated method.
 * This annotation is used for documentation only.
 */
@Retention(RetentionPolicy.SOURCE)
public @interface CodegenInvoke {
}
