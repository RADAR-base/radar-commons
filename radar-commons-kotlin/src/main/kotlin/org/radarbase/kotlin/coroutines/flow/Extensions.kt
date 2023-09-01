package org.radarbase.kotlin.coroutines.flow

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.zip

fun <T> List<Flow<T>>.zipAll(): Flow<List<T>> = when (val numberOfFlows = size) {
    0 -> flowOf(listOf())
    1 -> first().map { listOf(it) }
    2 -> first().zip(last()) { a, b -> listOf(a, b) }
    else -> subList(0, numberOfFlows - 1)
        .zipAll()
        .zip(last()) { rest, last -> rest + last }
}

fun <T> zipAll(
    vararg flows: Flow<T>,
): Flow<List<T>> = flows.toList().zipAll()
