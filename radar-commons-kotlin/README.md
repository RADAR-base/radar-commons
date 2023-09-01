# RADAR Commons Kotlin

This submodule provides helper functions for Kotlin code, mainly around Kotlin coroutines and Ktor authentication.

First of all, it contains a cache implementation to use with coroutines, with subtypes CachedMap, CachedSet and CachedValue

## Coroutine helpers

```kotlin
import java.io.IOException

val dirCache = CachedMap<String, FileInfo> {
    withContext(Dispatchers.IO) {
        client.fetchDirectoryMap()
    }
}

runBlocking {
    // will fetch the directory
    val myFileInfo = dirCache["myfile"]
    // will not fetch the directory but get it from cache
    val myFileInfo2 = dirCache["myfile2"] 
    try {
        // will fail to get it from cache, and re-read the directory
        // and finally return null. In the next few seconds, it will
        // not re-attempt a fetch, even for missing items.
        val nonExisting = dirCache["nonexisting"]
    } catch (ex: IOException) {
        // failed to fetch non-existing directory
    }
}
```

The usage of the other cached types is similar.

The directory also contains some coroutine helpers

`Semaphore.tryWithPermitOrNull`:

```kotlin
val semaphore = Semaphore(1)

val result = semaphore.tryWithPermitOrNull {
    modifyLimitedAccess()
}
if (result == null) {
    println("Did not modify")
} else {
    println("Got result $result")
}
```

`Future.suspendGet` is the suspending equivalent of `Future.get`.

`Iterable<T>.forkJoin` forks coroutines for all items in an iterable and suspends until they all have a result.

```kotlin
// delay for each value
(1..10)
    .forkJoin { i ->
        // because this suspends, the total amount waited should be
        // close to the maximum input (10 milliseconds)
        delay(i.milliseconds)
        i * 2
    }
    .forEach { println("Multiplied value is $it") }
```

`Iterable.launchJoin` is the similar, except it doesn't return a result.

`consumeFirst` is a race between producers to produce the first value. At least one producer should produce a value

```kotlin
val result = consumeFirst { emit ->
    launch {
        delay(Random.nextInt(100).milliseconds)
        emit("a")
    }
    launch {
        delay(Random.nextInt().milliseconds)
        emit("b")
    }
}
// Will randomly vary between "a" and "b"
println(result)
```

This race is used in `forkFirstOfOrNull`, `forkFirstOfNotNullOrNull`, `forkAny`

```kotlin
val resource = listOf(server1, server2)
    .forkFirstOfOrNull(
        transform = { server ->
            fetchFrom(server)
        },
        predicate = { it.isValid() }
    )
if (resource == null) {
    // none of the servers had the resource
}
```

## Ktor helpers

The methods and classes in `org.radarbase.ktor.auth` help setup client credentials authentication in Ktor as used in RADAR-base.

```kotlin
val client = HttpClient(CIO) {
    install(Auth) {
        clientCredentials(
            config = ClientCredentialsConfig(
                tokenUrl = "...",
                clientId = "...",
            ).copyWithEnv(),
            targetHost = "my.server",
        )
    }
}
```
