package org.radarbase.data;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.radarbase.producer.rest.AvroDataMapper;
import org.radarbase.producer.rest.AvroDataMapperFactory;
import org.radarbase.producer.rest.ParsedSchemaMetadata;

/**
 * Encodes data according to an Avro schema to the format and schema of the server.
 */
public class RemoteSchemaEncoder implements AvroEncoder {
    private final boolean binary;

    /**
     * Schema encoder.
     * @param binary true if the server wants binary encoding, false if it wants JSON encoding.
     */
    public RemoteSchemaEncoder(boolean binary) {
        this.binary = binary;
    }

    @Override
    public <T> AvroWriter<T> writer(Schema schema, Class<? extends T> clazz) {
        return new SchemaEncoderWriter<>(schema, clazz);
    }

    private class SchemaEncoderWriter<T> implements AvroWriter<T> {
        private final AvroEncoder recordEncoder;
        private AvroEncoder.AvroWriter<Object> encoder;
        private final boolean isGeneric;
        private ParsedSchemaMetadata serverSchema;
        private AvroDataMapper mapper;
        private final Schema schema;

        SchemaEncoderWriter(Schema schema, Class<? extends T> clazz) {
            this.schema = schema;

            GenericData genericData;
            if (SpecificRecord.class.isAssignableFrom(clazz)) {
                genericData = new SpecificData(RemoteSchemaEncoder.class.getClassLoader());
                isGeneric = false;
            } else {
                genericData = new GenericData(RemoteSchemaEncoder.class.getClassLoader());
                isGeneric = true;
            }
            recordEncoder = new AvroDatumEncoder(genericData, binary);
        }

        @Override
        public byte[] encode(T object) throws IOException {
            return encoder.encode(mapper.convertAvro(object));
        }

        @Override
        public final void setReaderSchema(ParsedSchemaMetadata readerSchema)
                throws SchemaValidationException {
            if (this.serverSchema != null
                    && readerSchema.getSchema().equals(this.serverSchema.getSchema())) {
                return;
            }
            try {
                if (!isGeneric) {
                    this.mapper = AvroDataMapperFactory.IDENTITY_MAPPER;
                    encoder = recordEncoder.writer(schema, Object.class);
                } else {
                    this.mapper = AvroDataMapperFactory.get()
                            .createMapper(schema, readerSchema.getSchema(),
                                    null);
                    encoder = recordEncoder.writer(readerSchema.getSchema(), Object.class);
                }
                this.serverSchema = readerSchema;
            } catch (IOException ex) {
                throw new IllegalStateException("Cannot construct Avro writer", ex);
            }
        }

        @Override
        public ParsedSchemaMetadata getReaderSchema() {
            return serverSchema;
        }
    }
}
