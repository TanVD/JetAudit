package serializers

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.records.ObjectState

internal class StringSerializerTest {

    @Test
    fun serializeString_serializedAsExpected() {
        val string: String = "string"
        val serialized = StringPresenter.serialize(string)
        Assert.assertEquals(serialized, getObjectState("string"))
    }

    @Test
    fun deserializeString_deserializedAsExpected() {
        val serialized = getObjectState("string")
        val string = StringPresenter.deserialize(serialized)
        Assert.assertEquals(string, "string")
    }

    fun getObjectState(value: String): ObjectState {
        return ObjectState(mapOf(StringPresenter.value to value))
    }
}