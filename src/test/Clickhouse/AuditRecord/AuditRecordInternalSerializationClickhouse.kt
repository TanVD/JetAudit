package Clickhouse.AuditRecord

import org.testng.Assert
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.descriptionColumn
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.unixTimeStampColumn
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.implementation.clickhouse.model.DbColumn
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbRow
import tanvd.audit.model.external.AuditSerializer
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.external.AuditType.TypesResolution.addType
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer
import java.util.*

internal class AuditRecordInternalSerializationClickhouse {

    class TestClassFirst {
        companion object serializer : AuditSerializer<TestClassFirst> {
            override fun display(value: TestClassFirst): String {
                return "TestClassFirstDisplay"
            }

            override fun deserialize(serializedString: String): TestClassFirst {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassFirst): String {
                return "TestClassFirstId"
            }

        }
    }

    class TestClassSecond {
        companion object serializer : AuditSerializer<TestClassSecond> {
            override fun display(value: TestClassSecond): String {
                return "TestClassSecondDisplay"
            }

            override fun deserialize(serializedString: String): TestClassSecond {
                throw UnsupportedOperationException("not implemented")
            }

            override fun serialize(value: TestClassSecond): String {
                return "TestClassSecondId"
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
    fun serializeArray_PrimitiveTypesDifferent_serializedAsExpected() {
        val auditRecord = AuditRecordInternal(ArrayList(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(Int::class), "123"))), 127)
        val row = ClickhouseRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(row, DbRow(arrayListOf(
                DbColumn("String", listOf("1234"), DbColumnType.DbArrayString),
                DbColumn("Int", listOf("123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("String", "Int"), DbColumnType.DbArrayString),
                DbColumn(unixTimeStampColumn, listOf("127"), DbColumnType.DbInt))))
    }

    @Test
    fun serializeArray_PrimitiveTypesCoincident_serializedAsExpected() {
        val auditRecord = AuditRecordInternal(ArrayList(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(String::class), "123"))), 127)
        val row = ClickhouseRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(row, DbRow(arrayListOf(
                DbColumn("String", listOf("1234", "123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("String", "String"), DbColumnType.DbArrayString),
                DbColumn(unixTimeStampColumn, listOf("127"), DbColumnType.DbInt))))
    }

    @Test
    fun serializeArray_NonPrimitiveTypesDifferent_serializedAsExpected() {
        val auditRecord = AuditRecordInternal(ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassSecond::class), "123"))), 127)
        val row = ClickhouseRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(row, DbRow(arrayListOf(
                DbColumn("TestClassFirst", listOf("1234"), DbColumnType.DbArrayString),
                DbColumn("TestClassSecond", listOf("123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("TestClassFirst", "TestClassSecond"), DbColumnType.DbArrayString),
                DbColumn(unixTimeStampColumn, listOf("127"), DbColumnType.DbInt))))
    }

    @Test
    fun serializeArray_NonPrimitiveTypesCoincident_serializedAsExpected() {
        val auditRecord = AuditRecordInternal(ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassFirst::class), "123"))), 127)
        val row = ClickhouseRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(row, DbRow(listOf(
                DbColumn("TestClassFirst", listOf("1234", "123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("TestClassFirst", "TestClassFirst"), DbColumnType.DbArrayString),
                DbColumn(unixTimeStampColumn, listOf("127"), DbColumnType.DbInt))))
    }

    @Test
    fun deserializeArray_PrimitiveTypesDifferent_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(unixTimeStampColumn, listOf("127"), DbColumnType.DbInt),
                DbColumn(descriptionColumn, arrayListOf("String", "Int"), DbColumnType.DbArrayString),
                DbColumn("String", arrayListOf("1234"), DbColumnType.DbArrayString),
                DbColumn("Int", arrayListOf("123"), DbColumnType.DbArrayString)
        )))
        Assert.assertEquals(auditRecord, AuditRecordInternal(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(Int::class), "123")), 127))
    }

    @Test
    fun deserializeArray_PrimitiveTypesCoincident_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(unixTimeStampColumn, listOf("127"), DbColumnType.DbInt),
                DbColumn(descriptionColumn, arrayListOf("String", "String"), DbColumnType.DbArrayString),
                DbColumn("String", arrayListOf("1234", "123"), DbColumnType.DbArrayString)
        )))
        Assert.assertEquals(auditRecord, AuditRecordInternal(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(String::class), "123")), 127))
    }

    @Test
    fun deserializeArray_NonPrimitiveTypesDifferent_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(unixTimeStampColumn, listOf("127"), DbColumnType.DbInt),
                DbColumn(descriptionColumn, arrayListOf("TestClassFirst", "TestClassSecond"),
                        DbColumnType.DbArrayString),
                DbColumn("TestClassFirst", arrayListOf("1234"), DbColumnType.DbArrayString),
                DbColumn("TestClassSecond", arrayListOf("123"), DbColumnType.DbArrayString)
        )))

        Assert.assertEquals(auditRecord.objects, ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassSecond::class), "123"))))
        Assert.assertEquals(auditRecord.unixTimeStamp, 127)
    }

    @Test
    fun serializeDeserializeArray_NonPrimitiveTypes_deserializedAsExpected() {

        val arrayObjects = ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(AuditType.resolveType(String::class), "string"),
                Pair(AuditType.resolveType(Int::class), "27"),
                Pair(AuditType.resolveType(TestClassSecond::class), "TestClassSecondId")))

        val row = ClickhouseRecordSerializer.serialize(AuditRecordInternal(arrayObjects, 127))
        val deserializedRecord = ClickhouseRecordSerializer.deserialize(row)

        Assert.assertEquals(deserializedRecord, AuditRecordInternal(arrayObjects, 127))
    }
}
