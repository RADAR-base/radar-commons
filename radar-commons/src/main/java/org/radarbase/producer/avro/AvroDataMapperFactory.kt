package org.radarbase.producer.avro

import org.apache.avro.JsonProperties
import org.apache.avro.Schema
import org.apache.avro.SchemaValidationException
import org.apache.avro.generic.*
import org.radarbase.util.Base64Encoder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.*

object AvroDataMapperFactory {
    /**
     * Create a mapper for data in one Avro schema to that in another Avro schema.
     * @param from originating Avro schema
     * @param to resulting Avro schema
     * @param defaultVal default value as defined in an Avro record field,
     * may be null if there is no default value.
     * @return Avro data mapper
     * @throws SchemaValidationException if the given schemas are incompatible.
     */
    @Throws(SchemaValidationException::class)
    fun createMapper(from: Schema, to: Schema, defaultVal: Any?): AvroDataMapper {
        if (from == to) {
            logger.debug("Using identity schema mapping from {} to {}", from, to)
            return IDENTITY_MAPPER
        }
        logger.debug("Computing custom mapping from {} to {}", from, to)
        return try {
            if (to.type == Schema.Type.UNION || from.type == Schema.Type.UNION) {
                return mapUnion(from, to, defaultVal)
            }
            if (to.type == Schema.Type.ENUM || from.type == Schema.Type.ENUM) {
                return mapEnum(from, to, defaultVal)
            }
            when (to.type) {
                Schema.Type.INT, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.FLOAT ->
                    return mapNumber(from, to, defaultVal)
                else -> {}
            }
            when (from.type) {
                Schema.Type.RECORD -> mapRecord(from, to)
                Schema.Type.ARRAY -> mapArray(from, to)
                Schema.Type.MAP -> mapMap(from, to)
                Schema.Type.FIXED, Schema.Type.BYTES -> mapBytes(from, to, defaultVal)
                Schema.Type.INT, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.FLOAT ->
                    mapNumber(from, to, defaultVal)
                to.type -> IDENTITY_MAPPER
                else -> throw validationException(to, from, "Schema types of from and to don't match")
            }
        } catch (ex: SchemaValidationException) {
            defaultVal ?: throw ex
            if (defaultVal === JsonProperties.NULL_VALUE) {
                AvroDataMapper { null }
            } else {
                AvroDataMapper { defaultVal }
            }
        }
    }

    /** Map one union to another, or a union to non-union, or non-union to union.  */
    @Throws(SchemaValidationException::class)
    private fun mapUnion(from: Schema, to: Schema, defaultVal: Any?): AvroDataMapper {
        // Do not create a custom mapper for trivial changes.
        if (
            from.type == Schema.Type.UNION &&
            to.type == Schema.Type.UNION &&
            from.types.size == from.types.size
        ) {
            val matches = from.types.indices.all { i ->
                val fromType = from.types[i].type
                val toType = to.types[i].type
                fromType == toType && fromType.isPrimitive()
            }
            if (matches) {
                return IDENTITY_MAPPER
            }
        }
        val resolvedFrom = if (from.type == Schema.Type.UNION) {
            nonNullUnionSchema(from)
        } else {
            from
        }

        return if (from.type == Schema.Type.UNION && to.type != Schema.Type.UNION) {
            defaultVal ?: throw validationException(to, from, "Cannot map union to non-union without a default value")
            val actualDefault = getDefaultValue(defaultVal, to)
            val subMapper = createMapper(resolvedFrom, to, defaultVal)
            AvroDataMapper { obj ->
                if (obj == null) {
                    actualDefault
                } else {
                    subMapper.convertAvro(obj)
                }
            }
        } else {
            val toNonNull = nonNullUnionSchema(to)
            val unionMapper = createMapper(resolvedFrom, toNonNull, defaultVal)
            AvroDataMapper { obj ->
                obj ?: return@AvroDataMapper null
                unionMapper.convertAvro(obj)
            }
        }
    }

    /** Map an array to another.  */
    @Throws(SchemaValidationException::class)
    private fun mapArray(from: Schema, to: Schema): AvroDataMapper {
        if (to.type != Schema.Type.ARRAY) {
            throw validationException(to, from, "Cannot map array to non-array")
        }
        val subMapper = createMapper(from.elementType, to.elementType, null)
        return AvroDataMapper { obj ->
            obj.asAvroType<List<*>>(from, to).map { subMapper.convertAvro(it) }
        }
    }

    /** Map a map to another.  */
    @Throws(SchemaValidationException::class)
    private fun mapMap(from: Schema, to: Schema): AvroDataMapper {
        if (to.type != Schema.Type.MAP) {
            throw validationException(to, from, "Cannot map map to non-map")
        }
        val subMapper = createMapper(from.valueType, to.valueType, null)
        return AvroDataMapper { obj ->
            buildMap {
                obj.asAvroType<Map<*, *>>(from, to).forEach { (k, v) ->
                    put(k.toString(), subMapper.convertAvro(v))
                }
            }
        }
    }

    @Throws(SchemaValidationException::class)
    private fun mapBytes(from: Schema, to: Schema, defaultVal: Any?): AvroDataMapper {
        return if (from.type == Schema.Type.BYTES && to.type == Schema.Type.BYTES) {
            IDENTITY_MAPPER
        } else if (from.type == Schema.Type.FIXED && to.type == Schema.Type.FIXED &&
            from.fixedSize == to.fixedSize
        ) {
            IDENTITY_MAPPER
        } else if (from.type == Schema.Type.FIXED && to.type == Schema.Type.BYTES) {
            AvroDataMapper { `object` ->
                ByteBuffer.wrap(`object`.asAvroType<GenericData.Fixed>(from, to).bytes())
            }
        } else if (from.type == Schema.Type.BYTES && to.type == Schema.Type.FIXED) {
            defaultVal ?: throw validationException(to, from, "Cannot map bytes to fixed without default value")

            AvroDataMapper { `object`: Any? ->
                val bytes = `object`.asAvroType<ByteBuffer>(from, to).array()
                val value = if (bytes.size == to.fixedSize) {
                    bytes
                } else {
                    defaultVal as? ByteArray
                }
                GenericData.get().createFixed(null, value, to)
            }
        } else if (to.type == Schema.Type.STRING) {
            val encoder = Base64Encoder
            if (from.type == Schema.Type.FIXED) {
                AvroDataMapper { `object` ->
                    encoder.encode(`object`.asAvroType<GenericData.Fixed>(from, to).bytes())
                }
            } else {
                AvroDataMapper { `object` ->
                    encoder.encode(`object`.asAvroType<ByteBuffer>(from, to).array())
                }
            }
        } else {
            throw validationException(to, from, "Fixed type must be mapped to comparable byte size")
        }
    }

    @Throws(SchemaValidationException::class)
    private fun mapRecord(from: Schema, to: Schema): AvroDataMapper {
        if (to.type != Schema.Type.RECORD) {
            throw validationException(to, from, "From and to schemas must be records.")
        }
        val fromFields = from.fields
        val toFields = arrayOfNulls<Schema.Field?>(
            fromFields.size,
        )
        val fieldMappers = arrayOfNulls<AvroDataMapper?>(
            fromFields.size,
        )
        val filledPositions = BooleanArray(to.fields.size)
        for (i in fromFields.indices) {
            val fromField = fromFields[i]
            val toField = to.getField(fromField.name()) ?: continue
            filledPositions[toField.pos()] = true
            toFields[i] = toField
            fieldMappers[i] = createMapper(
                fromField.schema(),
                toField.schema(),
                toField.defaultVal(),
            )
        }
        filledPositions.forEachIndexed { i, isFilled ->
            if (!isFilled && to.fields[i].defaultVal() == null) {
                throw validationException(
                    to,
                    from,
                    "Cannot map to record without default value for new field ${to.fields[i].name()}",
                )
            }
        }
        return RecordMapper(to, toFields, fieldMappers)
    }

    /** Maps one record to another.  */
    private class RecordMapper constructor(
        private val toSchema: Schema,
        private val toFields: Array<Schema.Field?>,
        private val fieldMappers: Array<AvroDataMapper?>,
    ) : AvroDataMapper {
        override fun convertAvro(`object`: Any?): GenericRecord {
            val builder = GenericRecordBuilder(toSchema)
            val record = `object`.asAvroType<IndexedRecord>(toSchema, toSchema)
            for (i in toFields.indices) {
                val field = toFields[i] ?: continue
                val mapper = fieldMappers[i] ?: continue
                builder[field] = mapper.convertAvro(record[i])
            }
            return builder.build()
        }

        override fun toString(): String {
            return (
                "RecordMapper{" +
                    "fieldMappers=" + fieldMappers.contentToString() +
                    ", toFields=" + toFields.contentToString() + '}'
                )
        }
    }

    private class StringToNumberMapper(
        private val defaultVal: Any?,
        private val mapping: (String) -> Number,
    ) :
        AvroDataMapper {
        override fun convertAvro(`object`: Any?): Any? {
            `object` ?: return defaultVal
            return try {
                mapping(`object`.toString())
            } catch (ex: NumberFormatException) {
                defaultVal
            }
        }
    }

    private val logger: Logger = LoggerFactory.getLogger(AvroDataMapperFactory::class.java)
    val IDENTITY_MAPPER: AvroDataMapper = object : AvroDataMapper {
        override fun convertAvro(`object`: Any?): Any? = `object`

        override fun toString(): String = "Identity"
    }

    private inline fun <reified T> Any?.asAvroType(from: Schema, to: Schema): T {
        if (this !is T) {
            throw validationException(
                to,
                from,
                "${to.type} type cannot be mapped from ${this?.javaClass?.name} Java type.",
            )
        }
        return this
    }

    private val PRIMITIVE_TYPES = EnumSet.of(
        Schema.Type.INT,
        Schema.Type.LONG,
        Schema.Type.BYTES,
        Schema.Type.FLOAT,
        Schema.Type.DOUBLE,
        Schema.Type.NULL,
        Schema.Type.BOOLEAN,
        Schema.Type.STRING,
    )

    /** Map one enum to another or to String.  */
    @Throws(SchemaValidationException::class)
    private fun mapEnum(from: Schema, to: Schema, defaultVal: Any?): AvroDataMapper {
        return if (to.type == Schema.Type.ENUM) {
            var containsAll = true
            if (from.type == Schema.Type.ENUM) {
                for (s in from.enumSymbols) {
                    if (!to.hasEnumSymbol(s)) {
                        containsAll = false
                        break
                    }
                }
            } else if (from.type == Schema.Type.STRING) {
                containsAll = false
            } else {
                throw validationException(to, from, "Cannot map enum from non-string or enum type")
            }
            if (containsAll) {
                AvroDataMapper { obj -> GenericData.EnumSymbol(to, obj.toString()) }
            } else {
                var defaultString = defaultVal as? String
                if (defaultString == null) {
                    if (to.hasEnumSymbol("UNKNOWN")) {
                        defaultString = "UNKNOWN"
                    } else {
                        throw validationException(
                            to,
                            from,
                            "Cannot map enum symbols without default value",
                        )
                    }
                }
                val symbol: GenericEnumSymbol<*> = GenericData.EnumSymbol(to, defaultString)
                AvroDataMapper { obj: Any? ->
                    val value = obj.toString()
                    if (to.hasEnumSymbol(value)) {
                        GenericData.EnumSymbol(to, value)
                    } else {
                        symbol
                    }
                }
            }
        } else if (from.type == Schema.Type.ENUM && to.type == Schema.Type.STRING) {
            AvroDataMapper { it.toString() }
        } else {
            throw validationException(to, from, "Cannot map unknown type with enum.")
        }
    }

    /** Get the default value as a Generic type.  */
    private fun getDefaultValue(defaultVal: Any?, schema: Schema): Any? {
        return if (defaultVal == null) {
            null
        } else if (schema.type == Schema.Type.ENUM) {
            GenericData.EnumSymbol(schema, defaultVal)
        } else {
            defaultVal
        }
    }

    /** Maps one number type to another or parses/converts to a string.  */
    @Throws(SchemaValidationException::class)
    private fun mapNumber(from: Schema, to: Schema, defaultVal: Any?): AvroDataMapper {
        if (from.type == to.type) {
            return IDENTITY_MAPPER
        }
        return if (from.type == Schema.Type.STRING) {
            defaultVal ?: throw validationException(to, from, "Cannot map string to number without default value.")
            when (to.type) {
                Schema.Type.INT -> StringToNumberMapper(defaultVal, Integer::valueOf)
                Schema.Type.LONG -> StringToNumberMapper(defaultVal, String::toLong)
                Schema.Type.DOUBLE -> StringToNumberMapper(defaultVal, String::toDouble)
                Schema.Type.FLOAT -> StringToNumberMapper(defaultVal, String::toFloat)
                else -> throw validationException(
                    to,
                    from,
                    "Cannot map numeric type with non-numeric type",
                )
            }
        } else {
            when (to.type) {
                Schema.Type.INT -> AvroDataMapper { it.asAvroType<Number>(from, to).toInt() }
                Schema.Type.LONG -> AvroDataMapper { it.asAvroType<Number>(from, to).toLong() }
                Schema.Type.DOUBLE -> AvroDataMapper { it.toString().toDouble() }
                Schema.Type.FLOAT -> AvroDataMapper { it.asAvroType<Number>(from, to).toFloat() }
                Schema.Type.STRING -> AvroDataMapper { it.toString() }
                else -> throw validationException(
                    to,
                    from,
                    "Cannot map numeric type with non-numeric type",
                )
            }
        }
    }

    /** Get the non-null union type of a nullable/optional union field.  */
    @Throws(SchemaValidationException::class)
    private fun nonNullUnionSchema(schema: Schema): Schema {
        val types = checkNotNull(schema.types) { "Union does not have subtypes" }
        if (types.size != 2) {
            throw validationException(schema, schema, "Types must denote optionals.")
        }
        return if (types[0].type == Schema.Type.NULL) {
            if (types[1].type != Schema.Type.NULL) {
                types[1]
            } else {
                throw validationException(schema, schema, "Types must denote optionals.")
            }
        } else if (types[1].type == Schema.Type.NULL) {
            types[0]
        } else {
            throw validationException(schema, schema, "Types must denote optionals.")
        }
    }

    private fun Schema.Type.isPrimitive(): Boolean = this in PRIMITIVE_TYPES

    internal fun validationException(
        from: Schema,
        to: Schema,
        message: String,
    ): SchemaValidationException = SchemaValidationException(
        to,
        from,
        IllegalArgumentException(message),
    )
}
