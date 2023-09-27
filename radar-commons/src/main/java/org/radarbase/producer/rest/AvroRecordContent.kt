package org.radarbase.producer.rest

import io.ktor.http.ContentType
import io.ktor.http.content.OutgoingContent

interface AvroRecordContent {
    fun createContent(contentType: ContentType): OutgoingContent
}
