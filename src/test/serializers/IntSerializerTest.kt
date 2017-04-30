package serializers

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.model.external.serializers.IntSerializer

internal class IntSerializerTest {

    @Test
    fun serializeInt_fiveInt_serializedAsExpected() {
        val number: Int = 5
        val serialized: String = IntSerializer.serialize(number)
        Assert.assertEquals(serialized, "5")
    }

    @Test
    fun deserializeInt_fiveInt_deserializedAsExpected() {
        val serializedString: String = "5"
        val numberDeserialized: Int = IntSerializer.deserialize(serializedString)
        Assert.assertEquals(numberDeserialized, 5)
    }

    @Test
    fun serializeInt_maxInt_serializedAsExpected() {
        val number: Int = Int.MAX_VALUE
        val serialized: String = IntSerializer.serialize(number)
        Assert.assertEquals(serialized, Int.MAX_VALUE.toString())
    }

    @Test
    fun deserializeInt_maxInt_deserializedAsExpected() {
        val serializedString: String = Int.MAX_VALUE.toString()
        val numberDeserialized: Int = IntSerializer.deserialize(serializedString)
        Assert.assertEquals(numberDeserialized, Int.MAX_VALUE)
    }

    @Test
    fun serializeInt_minInt_serializedAsExpected() {
        val number: Int = Int.MIN_VALUE
        val serialized: String = IntSerializer.serialize(number)
        Assert.assertEquals(serialized, Int.MIN_VALUE.toString())
    }

    @Test
    fun deserializeInt_minInt_deserializedAsExpected() {
        val serializedString: String = Int.MIN_VALUE.toString()
        val numberDeserialized: Int = IntSerializer.deserialize(serializedString)
        Assert.assertEquals(numberDeserialized, Int.MIN_VALUE)
    }
}