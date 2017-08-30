/*
 * Copyright 2017 The Hyve and King's College London
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

package org.radarcns.util.serde;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.radarcns.key.MeasurementKey;
import org.radarcns.producer.rest.SchemaRetriever;
import org.radarcns.producer.rest.ParsedSchemaMetadata;

public class KafkaAvroSerializerTest {

    @Test
    public void serialize() throws Exception {
        testSerialization("this", Schema.create(Type.STRING));
        testSerialization(10, Schema.create(Type.INT));

        MeasurementKey key = new MeasurementKey("a", "b");
        testSerialization(key, key.getSchema());

        Schema genericSchema = Schema.createRecord(Arrays.asList(
                new Field("a", Schema.create(Type.STRING), "aaa", "salt"),
                new Field("b", Schema.create(Type.INT), "bb", (Object)null)
        ));
        GenericRecord record = new GenericData.Record(genericSchema);
        record.put("a", "nodefault");
        record.put("b", 10);
        testSerialization(record, genericSchema);
    }

    private void testSerialization(Object data, Schema schema)
            throws IOException, RestClientException {
        SchemaRetriever retriever = mock(SchemaRetriever.class);
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(retriever);
        serializer.configure(Collections.<String,Object>emptyMap(), true);
        when(retriever.getOrSetSchemaMetadata("bla", false, schema, -1)).thenReturn(new ParsedSchemaMetadata(10, 2, schema));

        byte[] result = serializer.serialize("bla", data);

        verify(retriever, times(1)).getOrSetSchemaMetadata("bla", false, schema, -1);

        SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
        io.confluent.kafka.serializers.KafkaAvroSerializer altSerializer = new io.confluent.kafka.serializers.KafkaAvroSerializer(
                registryClient);
        altSerializer.configure(Collections.singletonMap("schema.registry.url", "http://example.com:8081"), true);

        when(registryClient.register("bla-key", schema)).thenReturn(10);

        byte[] altResult = altSerializer.serialize("bla", data);

        verify(registryClient, times(1)).register("bla-key", schema);

        assertArrayEquals(altResult, result);
    }
}