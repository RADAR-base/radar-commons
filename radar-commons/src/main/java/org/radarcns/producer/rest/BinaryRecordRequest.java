/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.producer.rest;

import static org.radarcns.util.Strings.bytesToHex;

import java.io.IOException;
import okio.Buffer;
import okio.BufferedSink;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.radarcns.data.AvroEncoder.AvroWriter;
import org.radarcns.data.RecordData;
import org.radarcns.data.RemoteSchemaEncoder;
import org.radarcns.topic.AvroTopic;

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
    private final AvroWriter<V> valueEncoder;
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
        valueEncoder = new RemoteSchemaEncoder(true)
                .writer(topic.getValueSchema(), topic.getValueClass());
    }

    @Override
    public void writeToSink(BufferedSink sink) throws IOException {
        writeToSink(sink, Integer.MAX_VALUE);
    }

    private void writeToSink(BufferedSink sink, int maxLength) throws IOException {
        binaryEncoder = EncoderFactory.get().directBinaryEncoder(
                sink.outputStream(), binaryEncoder);
        binaryEncoder.startItem();
        binaryEncoder.writeInt(keyVersion);
        binaryEncoder.writeInt(valueVersion);

        // do not send project ID; it is encoded in the serialization
        binaryEncoder.writeIndex(0);
        // do not send user ID; it is encoded in the serialization
        binaryEncoder.writeIndex(0);
        String sourceId = ((IndexedRecord) records.getKey()).get(sourceIdPos).toString();
        binaryEncoder.writeString(sourceId);
        binaryEncoder.writeArrayStart();
        binaryEncoder.setItemCount(records.size());

        int curLength = 18 + sourceId.length();

        for (V record : records) {
            if (curLength >= maxLength) {
                return;
            }
            binaryEncoder.startItem();
            byte[] valueBytes = valueEncoder.encode(record);
            binaryEncoder.writeBytes(valueBytes);
            curLength += 4 + valueBytes.length;
        }
        binaryEncoder.writeArrayEnd();
        binaryEncoder.flush();
    }

    @Override
    public void reset() {
        records = null;
    }

    @Override
    public void prepare(ParsedSchemaMetadata keySchema, ParsedSchemaMetadata valueSchema,
            RecordData<K, V> records) throws SchemaValidationException {
        keyVersion = keySchema.getVersion() == null ? 0 : keySchema.getVersion();
        valueVersion = valueSchema.getVersion() == null ? 0 : valueSchema.getVersion();

        valueEncoder.setReaderSchema(valueSchema);

        this.records = records;
    }

    @Override
    public String content(int maxLength) throws IOException {
        Buffer buffer = new Buffer();
        writeToSink(buffer, maxLength / 2 - 2);
        return "0x" + bytesToHex(buffer.readByteArray());
    }
}
