package org.radarbase.util

/**
 * A marker annotations for DSLs.
 */
@DslMarker
@Target(
    AnnotationTarget.CLASS,
    AnnotationTarget.TYPEALIAS,
    AnnotationTarget.TYPE,
    AnnotationTarget.FUNCTION,
)
annotation class RadarProducerDsl
