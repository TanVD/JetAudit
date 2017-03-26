package serializers

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.serializers.StringSerializer

internal class StringSerializer {

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