package org.radarbase.producer.rest

import io.ktor.http.content.*

interface AvroRecordContent {
    fun createContent(): OutgoingContent
}
