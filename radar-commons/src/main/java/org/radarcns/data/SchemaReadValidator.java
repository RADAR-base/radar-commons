package org.radarcns.data;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.radarcns.producer.rest.ParsedSchemaMetadata;

import java.util.Collections;

/**
 * Validates whether a schema in the schema registry is compatible with a local schema.
 */
public class SchemaReadValidator {
    public static final SchemaValidator SCHEMA_VALIDATOR = new SchemaValidatorBuilder()
            .canReadStrategy()
            .validateLatest();
    private final Iterable<Schema> schemas;

    private int previousSchemaId;

    /**
     * Schema validator for a single local schema.
     * @param schema local schema that data will be written with.
     */
    public SchemaReadValidator(Schema schema) {
        schemas = Collections.singleton(schema);
    }

    /**
     * Validate that given schema can successfully read data that was written with the local schema.
     * @param schema schema metadata from the schema registry
     * @throws SchemaValidationException if the schemas are not compatible.
     */
    public void validate(ParsedSchemaMetadata schema) throws SchemaValidationException {
        if (schema.getId() == null || previousSchemaId != schema.getId()) {
            if (schema.getId() != null) {
                previousSchemaId = schema.getId();
            }
            SCHEMA_VALIDATOR.validate(schema.getSchema(), schemas);
        }
    }
}
