package test

import org.testng.Assert
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditSerializer
import tanvd.audit.model.AuditType
import tanvd.audit.model.AuditType.TypesResolution.addType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer
import java.util.*



internal class AuditSerializationTest {

    class TestClassFirst(){
        companion object serializer : AuditSerializer<TestClassFirst> {
            override fun deserialize(serializedString: String): TestClassFirst {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassFirst): String {
                throw UnsupportedOperationException("not implemented")
            }

        }
    }

    class TestClassSecond() {
        companion object serializer : AuditSerializer<TestClassFirst> {
            override fun deserialize(serializedString: String): TestClassFirst {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassFirst): String {
                throw UnsupportedOperationException("not implemented")
            }

        }
    }

    @Suppress("UNCHECKED_CAST")
    @BeforeClass
    fun initTypeSystem() {
        AuditType.addType(AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>)
        AuditType.addType(AuditType(String::class, "String", StringSerializer) as AuditType<Any>)
        addType(AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>)
        addType(AuditType(TestClassSecond::class, "TestClassSecond", TestClassSecond) as AuditType<Any>)
    }

    @Test
    fun serializeArray_PrimitiveTypes_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(Int::class), "123"))))
        val serializedString = AuditRecord.serialize(auditRecord)
        Assert.assertEquals(serializedString, "String1234Int123")
    }

    @Test
    fun serializeArray_NonPrimitiveTypes_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassSecond::class), "123"))))
        val serializedString = AuditRecord.serialize(auditRecord)
        Assert.assertEquals(serializedString, "TestClassFirst1234TestClassSecond123")
    }

    @Test
    fun deserializeArray_PrimitiveTypes_deserializedAsExpected() {
        val serializedString = "String1234Int123"
        val auditRecord = AuditRecord.deserialize(serializedString)
        Assert.assertEquals(auditRecord.objects,
                ArrayList(listOf(
                        Pair(AuditType.resolveType(String::class), "1234"),
                        Pair(AuditType.resolveType(Int::class), "123"))))
    }

    @Test
    fun deserializeArray_NonPrimitiveTypes_deserializedAsExpected() {
        val serializedString = "TestClassFirst1234TestClassSecond123"
        val auditRecord = AuditRecord.deserialize(serializedString)

        Assert.assertEquals(auditRecord.objects,
                ArrayList(listOf(
                        Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                        Pair(AuditType.resolveType(TestClassSecond::class), "123"))))
    }

    @Test
    fun serializeDeserializeArray_NonPrimitiveTypes_deserializedAsExpected() {

        val arrayObjects = ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(AuditType.resolveType(String::class), "have been called"),
                Pair(AuditType.resolveType(Int::class), "27"),
                Pair(AuditType.resolveType(String::class), " times by "),
                Pair(AuditType.resolveType(TestClassSecond::class), "TestClassSecondId")))

        val serializedString = AuditRecord.serialize(AuditRecord(arrayObjects))
        val deserializedRecord = AuditRecord.deserialize(serializedString)

        Assert.assertEquals(deserializedRecord.objects, arrayObjects)
    }
}
