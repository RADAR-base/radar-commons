package org.radarbase.producer.avro

import org.apache.avro.Schema
import org.apache.avro.SchemaValidationException
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.radarcns.kafka.ObservationKey
import java.io.ByteArrayOutputStream
import java.io.IOException

class AvroDataMapperFactoryTest {
    @Test
    @Throws(SchemaValidationException::class, IOException::class)
    fun mapRecord() {
        val actual = doMap(
            MEASUREMENT_KEY_SCHEMA,
            ObservationKey.getClassSchema(),
            "{\"userId\":\"u\", \"sourceId\": \"s\"}",
        )
        assertEquals("{\"projectId\":null,\"userId\":\"u\",\"sourceId\":\"s\"}", actual)
    }

    @Test
    @Throws(SchemaValidationException::class)
    fun mapRecordIncomplete() {
        assertThrows<SchemaValidationException> {
            AvroDataMapperFactory.createMapper(
                INCOMPLETE_MEASUREMENT_KEY_SCHEMA,
                ObservationKey.getClassSchema(),
                null,
            )
        }
    }

    @Test
    @Throws(SchemaValidationException::class, IOException::class)
    fun mapEnumLarger() {
        val actual = doMap(SMALL_ENUM_SCHEMA, LARGE_ENUM_SCHEMA, "{\"e\":\"A\"}")
        assertEquals("{\"e\":\"A\"}", actual)
    }

    @Test
    @Throws(SchemaValidationException::class)
    fun mapEnumSmaller() {
        assertThrows<SchemaValidationException> {
            AvroDataMapperFactory.createMapper(LARGE_ENUM_SCHEMA, SMALL_ENUM_SCHEMA, null)
        }
    }

    @Test
    @Throws(SchemaValidationException::class, IOException::class)
    fun mapEnumSmallerUnknown() {
        val actual = doMap(LARGE_ENUM_SCHEMA, UNKNOWN_ENUM_SCHEMA, "{\"e\":\"C\"}")
        assertEquals("{\"e\":\"UNKNOWN\"}", actual)
    }

    @Test
    @Throws(SchemaValidationException::class, IOException::class)
    fun mapEnumSmallerDefault() {
        val actual = doMap(LARGE_ENUM_SCHEMA, DEFAULT_ENUM_SCHEMA, "{\"e\":\"C\"}")
        assertEquals("{\"e\":\"A\"}", actual)
    }

    @Test
    @Throws(SchemaValidationException::class, IOException::class)
    fun mapAll() {
        val actual = doMap(
            ALL_TYPES_SCHEMA,
            ALL_TYPES_ALT_SCHEMA,
            "{" +
                "\"e\":\"A\"," +
                "\"i\":1," +
                "\"l\":2," +
                "\"d\":3.0," +
                "\"f\":4.0," +
                "\"sI\":\"5\"," +
                "\"sD\":\"6.5\"," +
                "\"sU\":null," +
                "\"sUi\":{\"string\":\"7\"}," +
                "\"sUe\":null," +
                "\"uS\":\"s\"," +
                "\"se2\":\"B\"," +
                "\"se3\":\"g\"," +
                "\"a\":[1,2]," +
                "\"m\":{\"a\":9}," +
                "\"fS\":\"ab\"," +
                "\"bS\":\"ab\"," +
                "\"fb\":\"ab\"," +
                "\"bf\":\"ab\"," +
                "\"bfd\":\"abc\"," +
                "\"unmapped\":10}",
        )
        assertEquals(
            "{" +
                "\"e\":\"A\"," +
                "\"i\":1," +
                "\"l\":2.0," +
                "\"d\":3.0," +
                "\"f\":4.0," +
                "\"sI\":5," +
                "\"sD\":6.5," +
                "\"sU\":\"\"," +
                "\"sUi\":{\"int\":7}," +
                "\"sUe\":\"A\"," +
                "\"uS\":{\"string\":\"s\"}," +
                "\"se2\":\"B\"," +
                "\"se3\":\"A\"," +
                "\"a\":[1.0,2.0]," +
                "\"m\":{\"a\":9.0}," +
                "\"fS\":\"YWI=\"," +
                "\"bS\":\"YWI=\"," +
                "\"fb\":\"ab\"," +
                "\"bf\":\"ab\"," +
                "\"bfd\":\"aa\"" +
                "}",
            actual,
        )
    }

    @Throws(IOException::class, SchemaValidationException::class)
    private fun doMap(from: Schema, to: Schema, value: String): String {
        val mapper = AvroDataMapperFactory.createMapper(from, to, null)
        val reader = GenericDatumReader<Any>(from)
        val decoder = DecoderFactory.get().jsonDecoder(from, value)
        val readValue = reader.read(null, decoder)
        val writer = GenericDatumWriter<Any>(to)
        val out = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().jsonEncoder(to, out)
        writer.write(mapper.convertAvro(readValue), encoder)
        encoder.flush()
        return out.toString("utf-8")
    }

    companion object {
        private val MEASUREMENT_KEY_SCHEMA = Schema.Parser().parse(
            "{" +
                "  \"namespace\": \"org.radarcns.key\"," +
                "  \"type\": \"record\"," +
                "  \"name\": \"MeasurementKey\"," +
                "  \"doc\": \"Measurement key in the RADAR-base project\"," +
                "  \"fields\": [" +
                "    {\"name\": \"userId\", \"type\": \"string\", \"doc\": \"user ID\"}," +
                "    {\"name\": \"sourceId\", \"type\": \"string\", \"doc\": \"device source ID\"}" +
                "  ]" +
                "}",
        )
        private val INCOMPLETE_MEASUREMENT_KEY_SCHEMA = Schema.Parser().parse(
            "{" +
                "  \"namespace\": \"org.radarcns.key\"," +
                "  \"type\": \"record\"," +
                "  \"name\": \"MeasurementKey\"," +
                "  \"doc\": \"Measurement key in the RADAR-base project\"," +
                "  \"fields\": [" +
                "    {\"name\": \"sourceId\", \"type\": \"string\", \"doc\": \"device source ID\"}" +
                "  ]" +
                "}",
        )
        private val SMALL_ENUM_SCHEMA = Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"E\",\"fields\":[" +
                "{\"name\": \"e\", \"type\": {\"type\": \"enum\", \"name\": \"Enum\", \"symbols\": [\"A\", \"B\"]}}" +
                "]}",
        )
        private val LARGE_ENUM_SCHEMA = Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"E\",\"fields\":[" +
                "{\"name\": \"e\", \"type\": {\"type\": \"enum\", \"name\": \"Enum\", \"symbols\": [\"A\", \"B\", \"C\"]}}" +
                "]}",
        )
        private val UNKNOWN_ENUM_SCHEMA = Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"E\",\"fields\":[" +
                "{\"name\": \"e\", \"type\": {\"type\": \"enum\", \"name\": \"Enum\", \"symbols\": [\"A\", \"B\", \"UNKNOWN\"]}}" +
                "]}",
        )
        private val DEFAULT_ENUM_SCHEMA = Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"E\",\"fields\":[" +
                "{\"name\": \"e\", \"type\": {\"type\": \"enum\", \"name\": \"Enum\", \"symbols\": [\"A\"]}, \"default\": \"A\"}" +
                "]}",
        )
        private val ALL_TYPES_SCHEMA = Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[" +
                "{\"name\": \"e\", \"type\": {\"type\": \"enum\", \"name\": \"Enum\", \"symbols\": [\"A\"]}, \"default\": \"A\"}," +
                "{\"name\": \"i\", \"type\": \"int\"}," +
                "{\"name\": \"l\", \"type\": \"long\"}," +
                "{\"name\": \"d\", \"type\": \"double\"}," +
                "{\"name\": \"f\", \"type\": \"float\"}," +
                "{\"name\": \"sI\", \"type\": \"string\"}," +
                "{\"name\": \"sD\", \"type\": \"string\"}," +
                "{\"name\": \"sU\", \"type\": [\"null\", \"string\"]}," +
                "{\"name\": \"sUi\", \"type\": [\"null\", \"string\"]}," +
                "{\"name\": \"sUe\", \"type\": [\"null\", {\"name\": \"SE\", \"type\": \"enum\", \"symbols\": [\"A\"]}]}," +
                "{\"name\": \"uS\", \"type\": \"string\"}," +
                "{\"name\": \"se2\", \"type\": \"string\"}," +
                "{\"name\": \"se3\", \"type\": \"string\"}," +
                "{\"name\": \"a\", \"type\": {\"type\":\"array\", \"items\": {\"type\": \"int\"}}}," +
                "{\"name\": \"m\", \"type\": {\"type\":\"map\", \"values\": {\"type\": \"int\"}}}," +
                "{\"name\": \"fS\", \"type\": {\"name\": \"f1\", \"type\":\"fixed\", \"size\": 2}}," +
                "{\"name\": \"bS\", \"type\": \"bytes\"}," +
                "{\"name\": \"fb\", \"type\": {\"name\": \"f2\",\"type\": \"fixed\", \"size\": 2}}," +
                "{\"name\": \"bf\", \"type\": \"bytes\"}," +
                "{\"name\": \"bfd\", \"type\": \"bytes\"}," +
                "{\"name\": \"unmapped\", \"type\": \"int\"}" +
                "]}",
        )
        private val ALL_TYPES_ALT_SCHEMA = Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[" +
                "{\"name\": \"e\", \"type\": {\"type\": \"enum\", \"name\": \"Enum\", \"symbols\": [\"A\", \"B\"]}, \"default\": \"A\"}," +
                "{\"name\": \"i\", \"type\": \"long\"}," +
                "{\"name\": \"l\", \"type\": \"double\"}," +
                "{\"name\": \"d\", \"type\": \"float\"}," +
                "{\"name\": \"f\", \"type\": \"double\"}," +
                "{\"name\": \"sI\", \"type\": \"int\", \"default\": 0}," +
                "{\"name\": \"sD\", \"type\": \"double\", \"default\": 0.0}," +
                "{\"name\": \"sU\", \"type\": \"string\", \"default\": \"\"}," +
                "{\"name\": \"sUi\", \"type\": [\"null\", \"int\"], \"default\":null}," +
                "{\"name\": \"sUe\", \"type\": {\"name\": \"SE\", \"type\": \"enum\", \"symbols\": [\"A\"]}, \"default\": \"A\"}," +
                "{\"name\": \"uS\", \"type\": [\"null\", \"string\"]}," +
                "{\"name\": \"se2\", \"type\": {\"name\": \"SE2\", \"type\": \"enum\", \"symbols\": [\"A\", \"B\"]}, \"default\": \"A\"}," +
                "{\"name\": \"se3\", \"type\": {\"name\": \"SE3\", \"type\": \"enum\", \"symbols\": [\"A\", \"B\"]}, \"default\": \"A\"}," +
                "{\"name\": \"a\", \"type\": {\"type\":\"array\", \"items\": {\"type\": \"float\"}}}," +
                "{\"name\": \"m\", \"type\": {\"type\":\"map\", \"values\": {\"type\": \"float\"}}}," +
                "{\"name\": \"fS\", \"type\": \"string\"}," +
                "{\"name\": \"bS\", \"type\": \"string\"}," +
                "{\"name\": \"fb\", \"type\": \"bytes\"}," +
                "{\"name\": \"bf\", \"type\": {\"name\": \"f3\",\"type\":\"fixed\", \"size\": 2}, \"default\": \"aa\"}," +
                "{\"name\": \"bfd\", \"type\": {\"name\": \"f4\",\"type\":\"fixed\", \"size\": 2}, \"default\": \"aa\"}" +
                "]}",
        )
    }
}
