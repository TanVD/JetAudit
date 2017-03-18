package Clickhouse.AuditRecord

import org.testng.Assert
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.implementation.clickhouse.model.DbColumn
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbRow
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditSerializer
import tanvd.audit.model.AuditType
import tanvd.audit.model.AuditType.TypesResolution.addType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer
import java.util.*

internal class AuditRecordSerialization {

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
        addType(AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>)
        addType(AuditType(String::class, "String", StringSerializer) as AuditType<Any>)
        addType(AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>)
        addType(AuditType(TestClassSecond::class, "TestClassSecond", TestClassSecond) as AuditType<Any>)
    }

    @Test
    fun serializeArray_PrimitiveTypesDifferent_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(Int::class), "123"))))
        val (serializedString, row) = ClickhouseRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(serializedString, "StringInt")
        Assert.assertEquals(row, DbRow(arrayListOf(
                DbColumn("String", listOf("1234"), DbColumnType.DbArrayString),
                DbColumn("Int", listOf("123"), DbColumnType.DbArrayString))))
    }

    @Test
    fun serializeArray_PrimitiveTypesCoincident_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(String::class), "123"))))
        val (serializedString, row) = ClickhouseRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(serializedString, "StringString")
        Assert.assertEquals(row, DbRow(arrayListOf(
                DbColumn("String", listOf("1234", "123"), DbColumnType.DbArrayString))))
    }

    @Test
    fun serializeArray_NonPrimitiveTypesDifferent_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassSecond::class), "123"))))
        val (serializedString, row) = ClickhouseRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(serializedString, "TestClassFirstTestClassSecond")
        Assert.assertEquals(row, DbRow(arrayListOf(
                DbColumn("TestClassFirst", listOf("1234"), DbColumnType.DbArrayString),
                DbColumn("TestClassSecond", listOf("123"), DbColumnType.DbArrayString))))
    }

    @Test
    fun serializeArray_NonPrimitiveTypesCoincident_serializedAsExpected() {
        val auditRecord = AuditRecord(ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassFirst::class), "123"))))
        val (serializedString, row) = ClickhouseRecordSerializer.serialize(auditRecord)
        Assert.assertEquals(serializedString, "TestClassFirstTestClassFirst")
        Assert.assertEquals(row, DbRow(arrayListOf(
                DbColumn("TestClassFirst", listOf("1234", "123"), DbColumnType.DbArrayString))))
    }

    @Test
    fun deserializeArray_PrimitiveTypesDifferent_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn("description", arrayListOf("StringInt"), DbColumnType.DbString),
                DbColumn("String", arrayListOf("1234"), DbColumnType.DbArrayString),
                DbColumn("Int", arrayListOf("123"), DbColumnType.DbArrayString)
        )))
        Assert.assertEquals(auditRecord.objects, ArrayList(listOf(
                        Pair(AuditType.resolveType(String::class), "1234"),
                        Pair(AuditType.resolveType(Int::class), "123"))))
    }

    @Test
    fun deserializeArray_PrimitiveTypesCoincident_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn("description", arrayListOf("StringString"), DbColumnType.DbString),
                DbColumn("String", arrayListOf("1234", "123"), DbColumnType.DbArrayString)
        )))
        Assert.assertEquals(auditRecord.objects, ArrayList(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(String::class), "123"))))
    }

    @Test
    fun deserializeArray_NonPrimitiveTypesDifferent_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn("description", arrayListOf("TestClassFirstTestClassSecond"), DbColumnType.DbString),
                DbColumn("TestClassFirst", arrayListOf("1234"), DbColumnType.DbArrayString),
                DbColumn("TestClassSecond", arrayListOf("123"), DbColumnType.DbArrayString)
        )))

        Assert.assertEquals(auditRecord.objects,ArrayList(listOf(
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

        val (serializedString, row) = ClickhouseRecordSerializer.serialize(AuditRecord(arrayObjects))
        row.columns.add(DbColumn("description", listOf(serializedString), DbColumnType.DbString))
        val deserializedRecord = ClickhouseRecordSerializer.deserialize(row)

        Assert.assertEquals(deserializedRecord.objects, arrayObjects)
    }
}
