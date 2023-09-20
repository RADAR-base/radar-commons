package org.radarbase.producer.rest

import io.ktor.http.content.*
import io.ktor.util.*
import io.ktor.utils.io.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.apache.avro.SchemaValidationException
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import org.radarbase.data.AvroRecordData
import org.radarbase.producer.schema.ParsedSchemaMetadata
import org.radarbase.topic.AvroTopic
import org.radarcns.kafka.ObservationKey
import org.radarcns.kafka.RecordSet
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse
import org.radarcns.passive.phone.PhoneAcceleration
import java.io.BufferedReader
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStreamReader
import java.nio.ByteBuffer
import java.util.zip.GZIPOutputStream

class BinaryRecordContentTest {
    @Test
    @Throws(SchemaValidationException::class, IOException::class)
    fun writeToStream() = runTest {
        val k = ObservationKey("test", "a", "b")
        val v = EmpaticaE4BloodVolumePulse(
            0.0,
            0.0,
            0.0f,
        )
        val t = AvroTopic(
            "t",
            k.schema,
            v.schema,
            k.javaClass,
            v.javaClass,
        )
        val request = BinaryRecordContent(
            AvroRecordData(t, k, listOf(v)),
            ParsedSchemaMetadata(2, 1, k.schema),
            ParsedSchemaMetadata(4, 2, v.schema),
        )

        val channel = ByteChannel()
        launch {
            val content = request.createContent(RestKafkaSender.KAFKA_REST_BINARY_ENCODING) as OutgoingContent.WriteChannelContent
            content.writeTo(channel)
            channel.close()
        }
        assertArrayEquals(EXPECTED, channel.toByteArray())
    }

    @Test
    @Throws(IOException::class)
    fun expectedMatchesRecordSet() {
        val recordSet = RecordSet.newBuilder()
            .setKeySchemaVersion(1)
            .setValueSchemaVersion(2)
            .setData(
                listOf(
                    ByteBuffer.wrap(
                        byteArrayOf(
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                        ),
                    ),
                ),
            )
            .setProjectId(null)
            .setUserId(null)
            .setSourceId("b")
            .build()
        val writer = SpecificDatumWriter<RecordSet>(RecordSet.`SCHEMA$`)
        val out = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(recordSet, encoder)
        encoder.flush()
        assertArrayEquals(EXPECTED, out.toByteArray())
    }

    @Test
    @Throws(IOException::class)
    fun testSize() {
        val writer = SpecificDatumWriter<PhoneAcceleration>(PhoneAcceleration.`SCHEMA$`)
        val records: MutableList<ByteBuffer> = ArrayList(540)
        requireNotNull(BinaryRecordContentTest::class.java.getResourceAsStream("android_phone_acceleration.csv"))
            .use { stream ->
                InputStreamReader(stream).use { reader ->
                    BufferedReader(reader).use { br ->
                        var line = br.readLine()
                        var encoder: BinaryEncoder? = null
                        while (line != null) {
                            val values = line.split(",".toRegex()).dropLastWhile { it.isEmpty() }
                                .toTypedArray()
                            val acc = PhoneAcceleration(
                                values[0].toDouble(),
                                values[1].toDouble(),
                                values[2].toFloat(),
                                values[3].toFloat(),
                                values[4].toFloat(),
                            )
                            val out = ByteArrayOutputStream()
                            encoder = EncoderFactory.get().binaryEncoder(out, encoder)
                            writer.write(acc, encoder)
                            encoder.flush()
                            records.add(ByteBuffer.wrap(out.toByteArray()))
                            line = br.readLine()
                        }
                    }
                }
            }
        val recordSet = RecordSet.newBuilder()
            .setKeySchemaVersion(1)
            .setValueSchemaVersion(2)
            .setData(records)
            .setProjectId(null)
            .setUserId(null)
            .setSourceId("596740ca-5875-4c97-87ab-a08405f36aff")
            .build()
        val recordWriter = SpecificDatumWriter<RecordSet>(RecordSet.`SCHEMA$`)
        val out = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        recordWriter.write(recordSet, encoder)
        encoder.flush()
        println("Size of record set with " + records.size + " entries: " + out.size())
        val gzippedOut = ByteArrayOutputStream()
        val gzipOut = GZIPOutputStream(gzippedOut)
        gzipOut.write(out.size())
        gzipOut.close()
        println("Gzipped size of record set with " + records.size + " entries: " + gzippedOut.size())
    }

    companion object {
        // note that positive numbers are multiplied by two in avro binary encoding, due to the
        // zig-zag encoding schema used.
        // See http://avro.apache.org/docs/1.8.1/spec.html#binary_encoding
        private val EXPECTED = byteArrayOf(
            2, // key version x2
            4, // value version x2
            0, // null project ID
            0, // null user ID
            2, 'b'.code.toByte(), // string length x2, sourceId
            2, // number of records x2
            40, // number of bytes in the first value x2
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // value
            0, // end of array
        )
    }
}
