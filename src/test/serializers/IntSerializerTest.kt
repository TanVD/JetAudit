package serializers

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.model.external.presenters.IntPresenter
import tanvd.audit.model.external.records.ObjectState

internal class IntSerializerTest {

    @Test
    fun serializeInt_fiveInt_serializedAsExpected() {
        val number: Int = 5
        val serialized: ObjectState = IntPresenter.serialize(number)
        Assert.assertEquals(serialized, getObjectState(number))
    }

    @Test
    fun deserializeInt_fiveInt_deserializedAsExpected() {
        val serialized: ObjectState = getObjectState(5)
        val numberDeserialized: Int = IntPresenter.deserialize(serialized)!!
        Assert.assertEquals(numberDeserialized, 5)
    }

    @Test
    fun serializeInt_maxInt_serializedAsExpected() {
        val number: Int = Int.MAX_VALUE
        val serialized: ObjectState = IntPresenter.serialize(number)
        Assert.assertEquals(serialized,  getObjectState(number))
    }

    @Test
    fun deserializeInt_maxInt_deserializedAsExpected() {
        val serialized = getObjectState(Int.MAX_VALUE)
        val numberDeserialized: Int = IntPresenter.deserialize(serialized)!!
        Assert.assertEquals(numberDeserialized, Int.MAX_VALUE)
    }

    @Test
    fun serializeInt_minInt_serializedAsExpected() {
        val number: Int = Int.MIN_VALUE
        val serialized: ObjectState = IntPresenter.serialize(number)
        Assert.assertEquals(serialized,  getObjectState(number))
    }

    @Test
    fun deserializeInt_minInt_deserializedAsExpected() {
        val serialized = getObjectState(Int.MIN_VALUE)
        val numberDeserialized: Int = IntPresenter.deserialize(serialized)!!
        Assert.assertEquals(numberDeserialized, Int.MIN_VALUE)
    }

    fun getObjectState(value: Int): ObjectState {
        return ObjectState(mapOf(IntPresenter.value to value.toString()))
    }
}