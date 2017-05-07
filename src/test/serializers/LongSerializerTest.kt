package serializers

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.model.external.presenters.LongPresenter
import tanvd.audit.model.external.records.ObjectState

internal class LongSerializerTest {
    @Test
    fun serializeLong_fiveLong_serializedAsExpected() {
        val number: Long = 5
        val serialized: ObjectState = LongPresenter.serialize(number)
        Assert.assertEquals(serialized, getObjectState(number))
    }

    @Test
    fun deserializeLong_fiveLong_deserializedAsExpected() {
        val serialized: ObjectState = getObjectState(5)
        val numberDeserialized: Long = LongPresenter.deserialize(serialized)!!
        Assert.assertEquals(numberDeserialized, 5L)
    }

    @Test
    fun serializeLong_maxLong_serializedAsExpected() {
        val number: Long = Long.MAX_VALUE
        val serialized: ObjectState = LongPresenter.serialize(number)
        Assert.assertEquals(serialized, getObjectState(number))
    }

    @Test
    fun deserializeLong_maxLong_deserializedAsExpected() {
        val serialized: ObjectState = getObjectState(Long.MAX_VALUE)
        val numberDeserialized: Long = LongPresenter.deserialize(serialized)!!
        Assert.assertEquals(numberDeserialized, Long.MAX_VALUE)
    }

    @Test
    fun serializeLong_minLong_serializedAsExpected() {
        val number: Long = Long.MIN_VALUE
        val serialized: ObjectState = LongPresenter.serialize(number)
        Assert.assertEquals(serialized, getObjectState(number))
    }

    @Test
    fun deserializeLong_minLong_deserializedAsExpected() {
        val serialized: ObjectState = getObjectState(Long.MIN_VALUE)
        val numberDeserialized: Long = LongPresenter.deserialize(serialized)!!
        Assert.assertEquals(numberDeserialized, Long.MIN_VALUE)
    }

    fun getObjectState(value: Long): ObjectState {
        return ObjectState(mapOf(LongPresenter.value to value.toString()))
    }
}
