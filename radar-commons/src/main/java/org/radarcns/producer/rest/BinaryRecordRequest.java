package org.radarcns.producer.rest;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.radarcns.data.AvroEncoder;
import org.radarcns.data.AvroRecordData;
import org.radarcns.data.RecordData;
import org.radarcns.topic.AvroTopic;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Encodes a record request as binary data, in the form of a RecordSet.
 * @param <K> record key type
 * @param <V> record value type
 */
public class BinaryRecordRequest<K, V> implements RecordRequest<K, V> {
    private int keyVersion;
    private int valueVersion;
    private RecordData<K, V> records;
    private BinaryEncoder binaryEncoder;
    private AvroEncoder.AvroWriter<V> valueEncoder;
    private int sourceIdPos;

    /**
     * Binary record request for given topic.
     * @param topic topic to send data for.
     * @throws SchemaValidationException if the key schema does not contain a
     *                                   {@code sourceId} field.
     * @throws IllegalArgumentException if the topic cannot be used to make a AvroWriter.
     */
    public BinaryRecordRequest(AvroTopic<K, V> topic) throws SchemaValidationException {
        if (topic.getKeySchema() == null  || topic.getKeySchema().getType() != Schema.Type.RECORD) {
            Schema keySchema = topic.getKeySchema();
            if (keySchema == null) {
                keySchema = Schema.create(Schema.Type.NULL);
            }
            throw new SchemaValidationException(keySchema, keySchema,
                    new IllegalArgumentException("Cannot use non-record key schema"));
        }
        Schema.Field sourceIdField = topic.getKeySchema().getField("sourceId");
        if (sourceIdField == null) {
            throw new SchemaValidationException(topic.getKeySchema(), topic.getKeySchema(),
                    new IllegalArgumentException("Cannot use binary encoder without a source ID."));
        } else {
            sourceIdPos = sourceIdField.pos();
        }

        try {
            valueEncoder = AvroRecordData.getEncoder(
                    topic.getValueSchema(), topic.getValueClass(), true);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot create encoder", e);
        }
    }

    @Override
    public void writeToStream(OutputStream out) throws IOException {
        binaryEncoder = EncoderFactory.get().binaryEncoder(out, binaryEncoder);
        binaryEncoder.startItem();
        binaryEncoder.writeInt(keyVersion);
        binaryEncoder.writeInt(valueVersion);

        String sourceId = ((IndexedRecord) records.getKey()).get(sourceIdPos).toString();
        binaryEncoder.writeString(sourceId);
        binaryEncoder.writeArrayStart();
        binaryEncoder.setItemCount(records.size());

        for (V record : records.values()) {
            binaryEncoder.startItem();
            binaryEncoder.writeBytes(valueEncoder.encode(record));
        }
        binaryEncoder.writeArrayEnd();
        binaryEncoder.flush();
    }

    @Override
    public void reset() {
        records = null;
    }

    @Override
    public void setKeySchemaMetadata(ParsedSchemaMetadata schema) {
        keyVersion = schema.getVersion() == null ? 0 : schema.getVersion();
    }

    @Override
    public void setValueSchemaMetadata(ParsedSchemaMetadata schema) {
        valueVersion = schema.getVersion() == null ? 0 : schema.getVersion();
    }

    @Override
    public void setRecords(RecordData<K, V> records) {
        this.records = records;
    }
}
