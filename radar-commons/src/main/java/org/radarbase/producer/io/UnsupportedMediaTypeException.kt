package org.radarbase.producer.io

import io.ktor.http.*
import java.io.IOException

class UnsupportedMediaTypeException(
    contentType: ContentType?,
    contentEncoding: String?,
) : IOException(
    "Unsupported media type ${contentType ?: "unknown"} with ${contentEncoding ?: "no"} encoding",
)
