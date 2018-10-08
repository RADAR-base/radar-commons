package org.radarcns.producer.rest;

import static org.apache.avro.JsonProperties.NULL_VALUE;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.radarcns.util.Base64;
import org.radarcns.util.Base64.Encoder;

@SuppressWarnings({"PMD"})
public final class AvroDataMapperFactory {
    public static final AvroDataMapper IDENTITY_MAPPER = new AvroDataMapper() {
        @Override
        public Object convertAvro(Object obj) {
            return obj;
        }

        @Override
        public String toString() {
            return "Identity";
        }
    };
    private static final AvroDataMapperFactory INSTANCE = new AvroDataMapperFactory();

    public static AvroDataMapperFactory get() {
        return INSTANCE;
    }

    /**
     * Create a mapper for data in one Avro schema to that in another Avro schema.
     * @param from originating Avro schema
     * @param to resulting Avro schema
     * @param defaultVal default value as defined in an Avro record field,
     *                   may be null if there is no default value.
     * @return Avro data mapper
     * @throws SchemaValidationException if the given schemas are incompatible.
     */
    public AvroDataMapper createMapper(Schema from, Schema to, final Object defaultVal)
            throws SchemaValidationException {
        if (from.equals(to)) {
            return IDENTITY_MAPPER;
        }

        try {
            if (to.getType() == Schema.Type.UNION || from.getType() == Schema.Type.UNION) {
                return mapUnion(from, to, defaultVal);
            }
            if (to.getType() == Schema.Type.ENUM || to.getType() == Schema.Type.ENUM) {
                return mapEnum(from, to, defaultVal);
            }

            switch (to.getType()) {
                case INT:
                case LONG:
                case DOUBLE:
                case FLOAT:
                    return mapNumber(from, to, defaultVal);
                default:
                    break;
            }
            switch (from.getType()) {
                case RECORD:
                    return mapRecord(from, to);
                case ARRAY:
                    return mapArray(from, to);
                case MAP:
                    return mapMap(from, to);
                case FIXED:
                case BYTES:
                    return mapBytes(from, to, defaultVal);
                case INT:
                case LONG:
                case DOUBLE:
                case FLOAT:
                    return mapNumber(from, to, defaultVal);
                default:
                    if (from.getType() != to.getType()) {
                        throw new SchemaValidationException(to, from, new IllegalArgumentException(
                                "Schema types of from and to don't match"));
                    }
                    return IDENTITY_MAPPER;
            }
        } catch (SchemaValidationException ex) {
            if (defaultVal != null) {
                if (defaultVal == NULL_VALUE) {
                    return new AvroDataMapper() {
                        @Override
                        public Object convertAvro(Object obj) {
                            return null;
                        }
                    };
                } else {
                    return new AvroDataMapper() {
                        @Override
                        public Object convertAvro(Object obj) {
                            return defaultVal;
                        }
                    };
                }
            } else {
                throw ex;
            }
        }
    }

    /** Map one enum to another or to String. */
    private static AvroDataMapper mapEnum(Schema from, final Schema to, Object defaultVal)
            throws SchemaValidationException {
        if (to.getType() == Schema.Type.ENUM) {
            boolean containsAll = true;
            if (from.getType() == Schema.Type.ENUM) {
                for (String s : from.getEnumSymbols()) {
                    if (!to.hasEnumSymbol(s)) {
                        containsAll = false;
                        break;
                    }
                }
            } else if (from.getType() == Schema.Type.STRING) {
                containsAll = false;
            } else {
                throw new SchemaValidationException(to, from, new IllegalArgumentException(
                        "Cannot map enum from non-string or enum type"));
            }
            if (containsAll) {
                return new AvroDataMapper() {
                    @Override
                    public Object convertAvro(Object obj) {
                        return new GenericData.EnumSymbol(to, obj.toString());
                    }
                };
            } else {
                String defaultString = (String) defaultVal;
                if (defaultString == null && to.hasEnumSymbol("UNKNOWN")) {
                    defaultString = "UNKNOWN";
                }
                if (defaultString == null) {
                    throw new SchemaValidationException(to, from, new IllegalArgumentException(
                            "Cannot map enum symbols without default value"));
                } else {
                    final GenericEnumSymbol symbol = new GenericData.EnumSymbol(to, defaultString);
                    return new AvroDataMapper() {
                        @Override
                        public Object convertAvro(Object obj) {
                            String value = obj.toString();
                            if (to.hasEnumSymbol(value)) {
                                return new GenericData.EnumSymbol(to, value);
                            } else {
                                return symbol;
                            }
                        }
                    };
                }
            }
        } else if (from.getType() == Schema.Type.ENUM && to.getType() == Schema.Type.STRING) {
            return new AvroDataMapper() {
                @Override
                public Object convertAvro(Object obj) {
                    return obj.toString();
                }
            };
        } else {
            throw new SchemaValidationException(to, from, new IllegalArgumentException(
                    "Cannot map unknown type with enum."));
        }
    }

    /** Get the default value as a Generic type. */
    private static Object getDefaultValue(Object defaultVal, Schema schema) {
        if (defaultVal == null) {
            return null;
        } else if (schema.getType() == Schema.Type.ENUM) {
            return new GenericData.EnumSymbol(schema, defaultVal);
        } else {
            return defaultVal;
        }
    }

    /** Maps one number type to another or parses/converts to a string. */
    private static AvroDataMapper mapNumber(Schema from, Schema to, final Object defaultVal)
            throws SchemaValidationException {
        if (from.getType() == to.getType()) {
            return IDENTITY_MAPPER;
        }

        if (from.getType() == Schema.Type.STRING) {
            if (defaultVal == null) {
                throw new SchemaValidationException(to, from, new IllegalArgumentException(
                        "Cannot map string to number without default value."));
            } else {
                switch (to.getType()) {
                    case INT:
                        return new StringToNumberMapper(defaultVal) {
                            @Override
                            public Number stringToNumber(String obj) {
                                return Integer.valueOf(obj);
                            }
                        };
                    case LONG:
                        return new StringToNumberMapper(defaultVal) {
                            @Override
                            public Number stringToNumber(String obj) {
                                return Long.valueOf(obj);
                            }
                        };
                    case DOUBLE:
                        return new StringToNumberMapper(defaultVal) {
                            @Override
                            public Number stringToNumber(String obj) {
                                return Double.valueOf(obj);
                            }
                        };
                    case FLOAT:
                        return new StringToNumberMapper(defaultVal) {
                            @Override
                            public Number stringToNumber(String obj) {
                                return Float.valueOf(obj);
                            }
                        };
                    default:
                        throw new SchemaValidationException(to, from, new IllegalArgumentException(
                                "Cannot map numeric type with non-numeric type"));
                }
            }
        } else {
            switch (to.getType()) {
                case INT:
                    return new AvroDataMapper() {
                        @Override
                        public Object convertAvro(Object obj) {
                            return ((Number) obj).intValue();
                        }
                    };
                case LONG:
                    return new AvroDataMapper() {
                        @Override
                        public Object convertAvro(Object obj) {
                            return ((Number) obj).longValue();
                        }
                    };
                case DOUBLE:
                    return new AvroDataMapper() {
                        @Override
                        public Object convertAvro(Object obj) {
                            return Double.valueOf(obj.toString());
                        }
                    };
                case FLOAT:
                    return new AvroDataMapper() {
                        @Override
                        public Object convertAvro(Object obj) {
                            return ((Number) obj).floatValue();
                        }
                    };
                case STRING:
                    return new AvroDataMapper() {
                        @Override
                        public Object convertAvro(Object obj) {
                            return obj.toString();
                        }
                    };
                default:
                    throw new SchemaValidationException(to, from, new IllegalArgumentException(
                            "Cannot map numeric type with non-numeric type"));
            }
        }
    }

    /** Get the non-null union type of a nullable/optional union field. */
    private static Schema nonNullUnionSchema(Schema schema) throws SchemaValidationException {
        List<Schema> types = schema.getTypes();

        if (types.size() != 2) {
            throw new SchemaValidationException(schema, schema,
                    new IllegalArgumentException("Types must denote optionals"));
        }

        if (types.get(0).getType() == Schema.Type.NULL) {
            if (types.get(1).getType() != Schema.Type.NULL) {
                return types.get(1);
            } else {
                throw new SchemaValidationException(schema, schema,
                        new IllegalArgumentException("Types must denote optionals"));
            }
        } else if (types.get(1).getType() == Schema.Type.NULL) {
            return types.get(0);
        } else {
            throw new SchemaValidationException(schema, schema,
                    new IllegalArgumentException("Types must denote optionals."));
        }
    }

    /** Map one union to another, or a union to non-union, or non-union to union. */
    private AvroDataMapper mapUnion(Schema from, Schema to, Object defaultVal)
            throws SchemaValidationException {
        Schema resolvedFrom = from.getType() == Schema.Type.UNION ? nonNullUnionSchema(from) : from;

        if (from.getType() == Schema.Type.UNION && to.getType() != Schema.Type.UNION) {
            if (defaultVal != null) {
                final Object actualDefault = getDefaultValue(defaultVal, to);
                final AvroDataMapper subMapper = createMapper(resolvedFrom, to, defaultVal);
                return new AvroDataMapper() {
                    @Override
                    public Object convertAvro(Object obj) {
                        if (obj == null) {
                            return actualDefault;
                        } else {
                            return subMapper.convertAvro(obj);
                        }
                    }
                };
            } else {
                throw new SchemaValidationException(to, from, new IllegalArgumentException(
                        "Cannot map union to non-union without a default value"));
            }
        } else {
            Schema toNonNull = nonNullUnionSchema(to);
            final AvroDataMapper unionMapper = createMapper(resolvedFrom, toNonNull, defaultVal);
            return new AvroDataMapper() {
                @Override
                public Object convertAvro(Object obj) {
                    if (obj == null) {
                        return null;
                    } else {
                        return unionMapper.convertAvro(obj);
                    }
                }
            };
        }
    }

    /** Map an array to another. */
    private AvroDataMapper mapArray(Schema from, Schema to)
            throws SchemaValidationException {
        if (to.getType() != Schema.Type.ARRAY) {
            throw new SchemaValidationException(to, from,
                    new IllegalArgumentException("Cannot map array to non-array"));
        }
        final AvroDataMapper subMapper = createMapper(from.getElementType(), to.getElementType(),
                null);
        return new AvroDataMapper() {
            @Override
            public Object convertAvro(Object obj) {
                List array = (List) obj;
                List<Object> toArray = new ArrayList<>(array.size());
                for (Object val : array) {
                    toArray.add(subMapper.convertAvro(val));
                }
                return toArray;
            }
        };
    }

    /** Map a map to another. */
    private AvroDataMapper mapMap(Schema from, Schema to) throws SchemaValidationException {
        if (to.getType() != Schema.Type.MAP) {
            throw new SchemaValidationException(to, from,
                    new IllegalArgumentException("Cannot map array to non-array"));
        }
        final AvroDataMapper subMapper = createMapper(from.getValueType(), to.getValueType(),
                null);
        return new AvroDataMapper() {
            @Override
            public Object convertAvro(Object obj) {
                @SuppressWarnings("unchecked")
                Map<String, ?> map = (Map<String, ?>) obj;
                Map<String, Object> toMap = new HashMap<>(map.size() * 4 / 3 + 1);
                for (Map.Entry<String, ?> entry : map.entrySet()) {
                    toMap.put(entry.getKey(), subMapper.convertAvro(entry.getValue()));
                }
                return toMap;
            }
        };
    }

    private AvroDataMapper mapBytes(Schema from, final Schema to, final Object defaultVal)
            throws SchemaValidationException {
        if (from.getType() == to.getType()
                && (from.getType() == Type.BYTES
                || (from.getType() == Type.FIXED && from.getFixedSize() == to.getFixedSize()))) {
            return IDENTITY_MAPPER;
        } else if (from.getType() == Type.FIXED && to.getType() == Schema.Type.BYTES) {
            return new AvroDataMapper() {
                @Override
                public Object convertAvro(Object object) {
                    return ByteBuffer.wrap(((Fixed)object).bytes());
                }
            };
        } else if (from.getType() == Type.BYTES && to.getType() == Type.FIXED) {
            if (defaultVal == null) {
                throw new SchemaValidationException(to, from, new IllegalArgumentException(
                        "Cannot map bytes to fixed without default value"));
            }
            return new AvroDataMapper() {
                @Override
                public Object convertAvro(Object object) {
                    byte[] bytes = ((ByteBuffer) object).array();
                    if (bytes.length == to.getFixedSize()) {
                        return GenericData.get().createFixed(null, bytes, to);
                    } else {
                        return GenericData.get().createFixed(null, (byte[]) defaultVal, to);
                    }
                }
            };
        } else if (to.getType() == Type.STRING) {
            final Encoder encoder = Base64.getEncoder();
            if (from.getType() == Type.FIXED) {
                return new AvroDataMapper() {
                    @Override
                    public Object convertAvro(Object object) {
                        return new String(encoder.encode(((Fixed) object).bytes()),
                                StandardCharsets.UTF_8);
                    }
                };
            } else {
                return new AvroDataMapper() {
                    @Override
                    public Object convertAvro(Object object) {
                        return new String(encoder.encode(((ByteBuffer) object).array()),
                                StandardCharsets.UTF_8);
                    }
                };
            }
        } else {
            throw new SchemaValidationException(to, from,
                    new IllegalArgumentException(
                            "Fixed type must be mapped to comparable byte size"));
        }
    }


    private AvroDataMapper mapRecord(Schema from, Schema to)
        throws SchemaValidationException {
        if (to.getType() != Schema.Type.RECORD) {
            throw new SchemaValidationException(to, from,
                new IllegalArgumentException("From and to schemas must be records."));
        }
        List<Schema.Field> fromFields = from.getFields();
        Schema.Field[] toFields = new Schema.Field[fromFields.size()];
        AvroDataMapper[] fieldMappers = new AvroDataMapper[fromFields.size()];

        boolean[] filledPositions = new boolean[to.getFields().size()];

        for (int i = 0; i < fromFields.size(); i++) {
            Schema.Field fromField = fromFields.get(i);
            Schema.Field toField = to.getField(fromField.name());
            if (toField == null) {
                continue;
            }

            filledPositions[toField.pos()] = true;

            Schema fromSchema = fromField.schema();
            Schema toSchema = toField.schema();

            toFields[i] = toField;
            fieldMappers[i] = createMapper(fromSchema, toSchema, toField.defaultVal());
        }

        for (int i = 0; i < filledPositions.length; i++) {
            if (!filledPositions[i] && to.getFields().get(i).defaultVal() == null) {
                throw new SchemaValidationException(to, from,
                    new IllegalArgumentException("Cannot map to record without default value"
                            + " for new field " + to.getFields().get(i).name()));
            }
        }

        return new RecordMapper(to, toFields, fieldMappers);
    }

    /** Maps one record to another. */
    private static class RecordMapper implements AvroDataMapper {
        private final AvroDataMapper[] fieldMappers;
        private final Schema.Field[] toFields;
        private final Schema toSchema;

        RecordMapper(Schema toSchema, Schema.Field[] toFields, AvroDataMapper[] fieldMappers) {
            this.toSchema = toSchema;
            this.fieldMappers = fieldMappers;
            this.toFields = toFields;
        }


        @Override
        public GenericRecord convertAvro(Object obj) {
            GenericRecordBuilder builder = new GenericRecordBuilder(toSchema);
            IndexedRecord record = (IndexedRecord) obj;
            for (int i = 0; i < toFields.length; i++) {
                Schema.Field field = toFields[i];
                if (field == null) {
                    continue;
                }
                builder.set(field, fieldMappers[i].convertAvro(record.get(i)));
            }
            return builder.build();
        }

        @Override
        public String toString() {
            return "RecordMapper{"
                    + "fieldMappers=" + Arrays.toString(fieldMappers)
                    + ", toFields=" + Arrays.toString(toFields) + '}';
        }
    }

    private abstract static class StringToNumberMapper implements AvroDataMapper {
        private final Object defaultVal;

        StringToNumberMapper(Object defaultVal) {
            this.defaultVal = defaultVal;
        }

        @Override
        public Object convertAvro(Object object) {
            try {
                return stringToNumber(object.toString());
            } catch (NumberFormatException ex) {
                return defaultVal;
            }
        }

        abstract Number stringToNumber(String toString);
    }
}
