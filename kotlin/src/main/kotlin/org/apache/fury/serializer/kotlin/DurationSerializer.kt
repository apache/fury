package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.memory.MemoryBuffer
import org.apache.fury.serializer.ImmutableSerializer
import org.apache.fury.serializer.Serializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit

class DurationSerializer(
    fury: Fury,
    needToWriteRef:Boolean
) : ImmutableSerializer<Duration>(
    fury,
    Duration::class.java,
    needToWriteRef) {

    constructor(
        fury: Fury,
    ) : this (fury, fury.config.isTimeRefIgnored)

    private val durationUnitSerializer: Serializer<DurationUnit> by lazy {
        fury.classResolver.getSerializer(DurationUnit::class.java)
    }

    override fun write(buffer: MemoryBuffer, value: Duration) {
        val unit = computeDurationUnitPrecision(value)
        durationUnitSerializer.write(buffer, unit)

        val rawValue: Long = when (unit) {
            DurationUnit.NANOSECONDS -> value.inWholeNanoseconds
            DurationUnit.MICROSECONDS -> value.inWholeMicroseconds
            DurationUnit.MILLISECONDS -> value.inWholeMilliseconds
            DurationUnit.SECONDS -> value.inWholeSeconds
            DurationUnit.MINUTES -> value.inWholeMinutes
            DurationUnit.HOURS -> value.inWholeHours
            DurationUnit.DAYS -> value.inWholeDays
        }

        buffer.writeInt64(rawValue)
    }

    override fun read(buffer: MemoryBuffer): Duration {
        val unit = durationUnitSerializer.read(buffer)
        val rawValue = buffer.readInt64()
        return when (unit) {
                DurationUnit.NANOSECONDS -> rawValue.nanoseconds
                DurationUnit.MICROSECONDS -> rawValue.microseconds
                DurationUnit.MILLISECONDS -> rawValue.milliseconds
                DurationUnit.SECONDS -> rawValue.seconds
                DurationUnit.MINUTES -> rawValue.minutes
                DurationUnit.HOURS -> rawValue.hours
                DurationUnit.DAYS -> rawValue.days
            }
    }

    private fun computeDurationUnitPrecision(value:Duration) :DurationUnit {
        if (value == Duration.ZERO) {
            return DurationUnit.NANOSECONDS
        }
        if (value.isInfinite()) {
            return DurationUnit.MILLISECONDS
        }

        value.absoluteValue.toComponents { days, hours, minutes, seconds, nanoseconds ->
            val millisecondPrecision = nanoseconds >= 1_000_000 && nanoseconds % 1_000_000 == 0
            if (millisecondPrecision) {
                return DurationUnit.MILLISECONDS
            }
            val microsecondPrecision= nanoseconds >= 1_000 && nanoseconds % 1_000 == 0
            if (microsecondPrecision) {
                return DurationUnit.MICROSECONDS
            }
            val nanoSecondPrecision= nanoseconds != 0 && nanoseconds % 1000 != 0
            if (nanoSecondPrecision) {
                return DurationUnit.NANOSECONDS
            }

            if (seconds != 0) {
                return DurationUnit.SECONDS
            }

            if (minutes != 0) {
                return DurationUnit.MINUTES
            }

            if (hours != 0) {
                return DurationUnit.HOURS
            }

            // Return lowest precision unit.
            return DurationUnit.DAYS
        }
    }
}
