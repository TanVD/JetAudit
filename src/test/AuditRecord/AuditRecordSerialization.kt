package test

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.audit.AuditRecord
import java.util.*


internal class AuditSerializationTest {

    class TestClassFirst()

    class TestClassSecond()

    @Test
    fun serializeArray_PrimitiveTypes_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(Pair(String::class, "1234"), Pair(Int::class, "123"))))
        val serializedString = AuditRecord.serialize(auditRecord)
        Assert.assertEquals(serializedString, "1234kotlin.Int123")
    }

    @Test
    fun serializeArray_NonPrimitiveTypes_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(Pair(TestClassFirst::class, "1234"),
                Pair(TestClassSecond::class, "123"))))
        val serializedString = AuditRecord.serialize(auditRecord)
        Assert.assertEquals(serializedString, "test.AuditSerializationTest.TestClassFirst1234" +
                "test.AuditSerializationTest.TestClassSecond123")
    }

    @Test
    fun deserializeArray_PrimitiveTypes_deserializedAsExpected() {
        val serializedString = "1234kotlin.Int123"
        val types = listOf(String::class, Int::class)
        val auditRecord = AuditRecord.deserialize(serializedString, types)
        Assert.assertEquals(auditRecord.objects,
                ArrayList(listOf(Pair(String::class, "1234"),Pair(Int::class, "123"))))
    }

    @Test
    fun deserializeArray_NonPrimitiveTypes_deserializedAsExpected() {
        val serializedString = "test.AuditSerializationTest.TestClassFirst1234" +
                "test.AuditSerializationTest.TestClassSecond123"
        val types = listOf(TestClassFirst::class, TestClassSecond::class)
        val auditRecord = AuditRecord.deserialize(serializedString, types)

        Assert.assertEquals(auditRecord.objects,
                ArrayList(listOf(Pair(TestClassFirst::class, "1234"), Pair(TestClassSecond::class, "123"))))
    }

    @Test
    fun serializeDeserializeArray_NonPrimitiveTypes_deserializedAsExpected() {
        val arrayObjects = ArrayList(listOf(Pair(TestClassFirst::class, "TestClassFirstId"),
                Pair(String::class, "have been called"), Pair(Int::class, "27"), Pair(String::class, " times by "),
                Pair(TestClassFirst::class, "TestClassSecondId")))
        val types = listOf(TestClassFirst::class, TestClassSecond::class, String::class, Int::class)
        val serializedString = AuditRecord.serialize(AuditRecord(arrayObjects))
        val deserializedRecord = AuditRecord.deserialize(serializedString, types)

        Assert.assertEquals(deserializedRecord.objects, arrayObjects)
    }
}
