package serializers

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.model.external.serializers.StringSerializer

internal class StringSerializerTest {

    @Test
    fun serializeString_serializedAsExpected() {
        val string: String = "string"
        val serialized = StringSerializer.serialize(string)
        Assert.assertEquals(serialized, "string")
    }

    @Test
    fun deserializeString_deserializedAsExpected() {
        val serializedString = "string"
        val string = StringSerializer.deserialize(serializedString)
        Assert.assertEquals(string, "string")
    }
}