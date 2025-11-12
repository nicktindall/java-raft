package au.id.tindall.distalg.raft.serialisation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Indicates that the static method is used to parse instances of the class it belongs to
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target(java.lang.annotation.ElementType.METHOD)
public @interface Parser {
}
