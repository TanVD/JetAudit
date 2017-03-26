package MySQL.AuditRecord

import org.testng.Assert
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl.Scheme.mandatoryColumnsAuditTable
import tanvd.audit.implementation.mysql.MysqlRecordSerializer
import tanvd.audit.implementation.mysql.model.DbColumn
import tanvd.audit.implementation.mysql.model.DbRow
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditSerializer
import tanvd.audit.model.AuditType
import tanvd.audit.model.AuditType.TypesResolution.addType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer
import java.util.*

internal class AuditRecordSerializationMySQL {

    class TestClassFirst() {
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
        addType(AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>)
        addType(AuditType(String::class, "String", StringSerializer) as AuditType<Any>)
        addType(AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>)
        addType(AuditType(TestClassSecond::class, "TestClassSecond", TestClassSecond) as AuditType<Any>)
    }

    @Test
    fun serializeArray_PrimitiveTypes_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(Int::class), "123"))), 127)
        val row = MysqlRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(row, DbRow(
                DbColumn(mandatoryColumnsAuditTable.find { it.name == AuditDaoMysqlImpl.descriptionColumn }!!,
                        "String1234Int123"),
                DbColumn(mandatoryColumnsAuditTable.find { it.name == AuditDaoMysqlImpl.unixTimeStampColumn }!!,
                        "127")))
    }

    @Test
    fun serializeArray_NonPrimitiveTypes_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassSecond::class), "123"))), 127)
        val row = MysqlRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(row, DbRow(
                DbColumn(mandatoryColumnsAuditTable.find { it.name == AuditDaoMysqlImpl.descriptionColumn }!!,
                        "TestClassFirst1234TestClassSecond123"),
                DbColumn(mandatoryColumnsAuditTable.find { it.name == AuditDaoMysqlImpl.unixTimeStampColumn }!!,
                        "127")))
    }

    @Test
    fun deserializeArray_PrimitiveTypes_deserializedAsExpected() {
        val row = DbRow(
                DbColumn(mandatoryColumnsAuditTable.find { it.name == AuditDaoMysqlImpl.descriptionColumn }!!,
                        "String1234Int123"),
                DbColumn(mandatoryColumnsAuditTable.find { it.name == AuditDaoMysqlImpl.unixTimeStampColumn }!!,
                        "127"))
        val auditRecord = MysqlRecordSerializer.deserialize(row)
        Assert.assertEquals(auditRecord.objects,
                ArrayList(listOf(
                        Pair(AuditType.resolveType(String::class), "1234"),
                        Pair(AuditType.resolveType(Int::class), "123"))))
        Assert.assertEquals(auditRecord.unixTimeStamp, 127)
    }

    @Test
    fun deserializeArray_NonPrimitiveTypes_deserializedAsExpected() {
        val row = DbRow(
                DbColumn(mandatoryColumnsAuditTable.find { it.name == AuditDaoMysqlImpl.descriptionColumn }!!,
                        "TestClassFirst1234TestClassSecond123"),
                DbColumn(mandatoryColumnsAuditTable.find { it.name == AuditDaoMysqlImpl.unixTimeStampColumn }!!,
                        "127"))
        val auditRecord = MysqlRecordSerializer.deserialize(row)

        Assert.assertEquals(auditRecord.objects,
                ArrayList(listOf(
                        Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                        Pair(AuditType.resolveType(TestClassSecond::class), "123"))))
        Assert.assertEquals(auditRecord.unixTimeStamp, 127)

    }

    @Test
    fun serializeDeserializeArray_NonPrimitiveTypes_deserializedAsExpected() {

        val arrayObjects = ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(AuditType.resolveType(String::class), "have been called"),
                Pair(AuditType.resolveType(Int::class), "27"),
                Pair(AuditType.resolveType(String::class), " times by "),
                Pair(AuditType.resolveType(TestClassSecond::class), "TestClassSecondId")))

        val row = MysqlRecordSerializer.serialize(AuditRecord(arrayObjects, 127))
        val deserializedRecord = MysqlRecordSerializer.deserialize(row)

        Assert.assertEquals(deserializedRecord.objects, arrayObjects)

    }
}
