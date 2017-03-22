package serializers

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.serializers.LongSerializer

internal class LongSerializer {
    @Test
    fun serializeLong_fiveLong_serializedAsExpected() {
        val number: Long = 5L
        val serialized: String = LongSerializer.serialize(number)
        Assert.assertEquals(serialized, "5")
    }

    @Test
    fun deserializeLong_fiveLong_deserializedAsExpected() {
        val serializedString: String = "5"
        val numberDeserialized: Long = LongSerializer.deserialize(serializedString)
        Assert.assertEquals(numberDeserialized, 5L)
    }

    @Test
    fun serializeLong_maxLong_serializedAsExpected() {
        val number: Long = Long.MAX_VALUE
        val serialized: String = LongSerializer.serialize(number)
        Assert.assertEquals(serialized, Long.MAX_VALUE.toString())
    }

    @Test
    fun deserializeLong_maxLong_deserializedAsExpected() {
        val serializedString: String = Long.MAX_VALUE.toString()
        val numberDeserialized: Long = LongSerializer.deserialize(serializedString)
        Assert.assertEquals(numberDeserialized, Long.MAX_VALUE)
    }

    @Test
    fun serializeLong_minLong_serializedAsExpected() {
        val number: Long = Long.MIN_VALUE
        val serialized: String = LongSerializer.serialize(number)
        Assert.assertEquals(serialized, Long.MIN_VALUE.toString())
    }

    @Test
    fun deserializeLong_minLong_deserializedAsExpected() {
        val serializedString: String = Long.MIN_VALUE.toString()
        val numberDeserialized: Long = LongSerializer.deserialize(serializedString)
        Assert.assertEquals(numberDeserialized, Long.MIN_VALUE)
    }
}
