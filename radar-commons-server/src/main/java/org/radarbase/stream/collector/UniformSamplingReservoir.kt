package org.radarbase.stream.collector

import com.fasterxml.jackson.annotation.JsonGetter
import org.apache.avro.specific.SpecificRecord
import org.radarbase.util.SpecificAvroConvertible
import kotlin.math.min
import kotlin.random.Random

/**
 * Uniform sampling reservoir for streaming. This should capture the input distribution in order
 * to compute quartiles, using so-called Algorithm R.
 *
 *
 * The maximum size of the reservoir can be increased to get more accurate quartile estimations.
 * As long as the number of samples is lower than the maximum size of the reservoir, the quartiles
 * are computed exactly.
 */
class UniformSamplingReservoir @JvmOverloads constructor(
    samples: DoubleArray = doubleArrayOf(),
    count: Long = 0,
    maxSize: Int = MAX_SIZE_DEFAULT,
) : SpecificAvroConvertible {
    private var samples: DoubleArray = DoubleArray(maxSize)

    /** Get the maximum size of this reservoir.  */
    var maxSize: Int = maxSize
        private set

    /** Get the number of samples that are being represented by the reservoir.  */
    var count: Long = count
        private set

    @Transient
    private var currentLength = 0

    /**
     * Create a reservoir that samples from given values.
     * @param samples list of values to sample from.
     * @param count current size of the number of samples that the reservoir represents.
     * @param maxSize maximum reservoir size.
     * @throws NullPointerException if given allValues are `null`
     */
    /** Empty reservoir with default maximum size.  */
    init {
        initializeReservoir(samples, count, maxSize)
    }

    /** Sample from given list of samples to initialize the reservoir.  */
    private fun initializeReservoir(initSamples: DoubleArray, initCount: Long, initMaxSize: Int) {
        require(initSamples.size <= initCount) { "Reservoir count must be larger or equal than number of samples." }
        maxSize = initMaxSize
        samples = DoubleArray(initMaxSize)
        count = initCount
        if (initSamples.isEmpty()) {
            currentLength = 0
            return
        }
        subsample(initSamples)
        samples.sort(0, currentLength)
    }

    private fun subsample(initSamples: DoubleArray) {
        // There are much more samples than the size permits. Random sample from the
        // given list. Duplicate addresses are retried, and for each hit there is at least a
        // 50% probability of getting a number that has not yet been picked.
        currentLength = if (initSamples.size > maxSize * 2) {
            val indexSet: MutableSet<Int> = HashSet()
            var sampleIndex = 0

            while (sampleIndex < maxSize) {
                val initIndex = Random.nextInt(initSamples.size)
                // only add to samples if index set does not yet contain value
                if (indexSet.add(initIndex)) {
                    samples[sampleIndex] = initSamples[initIndex]
                    sampleIndex++
                }
            }
            maxSize

            // There are not much more samples than the size permits. Make a list from all indexes
            // and at random pick and remove index from that. Put all the samples at given
            // indexes in the reservoir. Do not do retry sampling as above, as the final entry may
            // have a probability of 1/maxSize of actually being picked.
        } else if (initSamples.size > maxSize) {
            val allInitIndexes = initSamples.indices.toMutableList()
            repeat(maxSize) { sampleIndex ->
                val initIndex = allInitIndexes.removeAt(Random.nextInt(allInitIndexes.size))
                samples[sampleIndex] = initSamples[initIndex]
            }
            maxSize
        } else {
            initSamples.copyInto(samples)
            initSamples.size
        }
    }

    /** Add a sample to the reservoir.  */
    fun add(value: Double) {
        if (currentLength == maxSize) {
            val removeIndex = Random.nextLong(count)
            if (removeIndex < maxSize) {
                removeAndAdd(removeIndex.toInt(), value)
            }
        } else {
            removeAndAdd(currentLength, value)
            currentLength++
        }
        count++
    }

    private fun removeAndAdd(removeIndex: Int, value: Double) {
        var addIndex = samples.binarySearch(value, 0, currentLength)
        if (addIndex < 0) {
            addIndex = -addIndex - 1
        }

        // start: [a, b, c, d, e]
        if (removeIndex < addIndex) {
            // removeIndex = 2, value = d2 -> addIndex = 4
            addIndex--
            // new addIndex -> 3
            // copy(3 -> 2, len 1)
            // end: [a, b, d, d2, e]
            if (removeIndex < addIndex) {
                samples.copyInto(
                    samples,
                    destinationOffset = removeIndex,
                    startIndex = removeIndex + 1,
                    endIndex = addIndex,
                )
            }
        } else if (removeIndex > addIndex) {
            // removeIndex = 2, value = a2 -> addIndex = 1
            // copy(1 -> 2, len 1)
            // end: [a, a2, b, d, e]
            samples.copyInto(
                samples,
                destinationOffset = addIndex + 1,
                startIndex = addIndex,
                endIndex = removeIndex,
            )
        }
        samples[addIndex] = value
    }

    val quartiles: List<Double>
        /**
         * Get the quartiles of the underlying distribution. If the number of samples is larger than
         * the maximum size of the reservoir, this will be an estimate.
         * @return list with size three, of the 25, 50 and 75 percentiles.
         */
        get() = (1..3)
            .map { i ->
                when (currentLength) {
                    0 -> Double.NaN
                    1 -> samples[0]
                    else -> {
                        val pos = i * (currentLength + 1) * 0.25 // 25 percentile steps
                        when (val intPos = pos.toInt()) {
                            0 -> samples[0]
                            currentLength -> samples[currentLength - 1]
                            else -> {
                                val diff = pos - intPos
                                val base = samples[intPos - 1]
                                base + diff * (samples[intPos] - base)
                            }
                        }
                    }
                }
            }

    /** Get the currently stored samples.  */
    @JsonGetter
    fun getSamples(): List<Double> = (0 until currentLength)
        .map { samples[it] }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as UniformSamplingReservoir
        return count == other.count &&
            maxSize == other.maxSize &&
            samples.contentEquals(other.samples)
    }

    override fun hashCode(): Int {
        var result = samples.contentHashCode()
        result = 31 * result + maxSize
        result = 31 * result + count.hashCode()
        return result
    }

    override fun toString(): String = "UniformSamplingReservoir{" +
        "samples=${samples.contentToString()}, " +
        "maxSize=$maxSize, " +
        "count=$count}"

    override fun toAvro(): SamplingReservoirState {
        val length = min(maxSize.toLong(), count).toInt()

        return SamplingReservoirState().apply {
            this.count = this@UniformSamplingReservoir.count
            this.maxSize = this@UniformSamplingReservoir.maxSize
            this.samples = (0 until length).map { this@UniformSamplingReservoir.samples[it] }
        }
    }

    override fun fromAvro(record: SpecificRecord) {
        require(record is SamplingReservoirState) { "Cannot initialize from non-samplingreservoirstate" }

        val stateSamples = requireNotNull(record.samples) { "Samples may not be null" }
        initializeReservoir(
            stateSamples.toDoubleArray(),
            record.count,
            record.maxSize,
        )
    }

    companion object {
        private const val MAX_SIZE_DEFAULT = 999
    }
}
