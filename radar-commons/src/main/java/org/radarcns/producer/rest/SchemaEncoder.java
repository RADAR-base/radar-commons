package org.radarcns.producer.rest;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.AvroEncoder;
import org.radarcns.data.GenericRecordEncoder;
import org.radarcns.data.SpecificRecordEncoder;

/**
 * Encodes data according to an Avro schema to the format and schema of the server.
 */
public class SchemaEncoder<T> {
    private final AvroEncoder recordEncoder;

    private AvroEncoder.AvroWriter<Object> encoder;
    private final Schema schema;
    private ParsedSchemaMetadata serverSchema;
    private AvroDataMapper mapper;
    private final boolean isGeneric;

    /**
     * Schema encoder.
     * @param originalSchema schema that the data comes in.
     * @param clz class type of the data.
     * @param binary true if the server wants binary encoding, false if it wants JSON encoding.
     */
    public SchemaEncoder(Schema originalSchema, Class<? extends T> clz, boolean binary) {
        this.schema = originalSchema;
        if (SpecificRecord.class.isAssignableFrom(clz)) {
            recordEncoder = new SpecificRecordEncoder(binary);
            isGeneric = false;
        } else {
            recordEncoder = new GenericRecordEncoder(binary);
            isGeneric = true;
        }
    }

    /**
     * Update the server schema for the current data type.
     * @param serverSchema schema metadata returned by the schema registry
     * @throws SchemaValidationException if the given server schema is incompatible with the local
     *                                   schema.
     * @throws IOException if the Avro writer cannot be constructed.
     */
    public final void updateServerSchema(ParsedSchemaMetadata serverSchema)
            throws SchemaValidationException, IOException {
        if (this.serverSchema != null
                && serverSchema.getSchema().equals(this.serverSchema.getSchema())) {
            return;
        }
        this.serverSchema = serverSchema;
        if (!isGeneric) {
            this.mapper = AvroDataMapperFactory.IDENTITY_MAPPER;
            encoder = recordEncoder.writer(schema, Object.class);
        } else {
            this.mapper = AvroDataMapperFactory.get().createMapper(schema, serverSchema.getSchema(),
                    null);
            encoder = recordEncoder.writer(serverSchema.getSchema(), Object.class);
        }
    }

    /**
     * Converts an object to the server schema and encodes it.
     * @param value local value
     * @return converted and serialized data
     * @throws IOException if the conversion or serialization fails.
     */
    public byte[] convertAndEncode(T value) throws IOException {
        return encoder.encode(mapper.convertAvro(value));
    }

    public ParsedSchemaMetadata getServerSchema() {
        return serverSchema;
    }
}
