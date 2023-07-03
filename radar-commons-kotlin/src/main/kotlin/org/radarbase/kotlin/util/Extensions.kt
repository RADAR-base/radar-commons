package org.radarbase.kotlin.util

fun String?.removeSensitive(): String = if (this == null) "null" else "***"
