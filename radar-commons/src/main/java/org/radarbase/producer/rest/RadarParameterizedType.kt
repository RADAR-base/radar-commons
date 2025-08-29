package org.radarbase.producer.rest

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

class RadarParameterizedType(
    private val raw: Class<*>,
    private val args: Array<Type>,
    private val owner: Type? = null,
) : ParameterizedType {
    override fun getRawType(): Type = raw
    override fun getActualTypeArguments(): Array<Type> = args
    override fun getOwnerType(): Type? = owner

    override fun toString(): String {
        return buildString {
            append(raw.typeName)
            if (args.isNotEmpty()) {
                append('<')
                append(args.joinToString(", ") { it.typeName })
                append('>')
            }
        }
    }
}
