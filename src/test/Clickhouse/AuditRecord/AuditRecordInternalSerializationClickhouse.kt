package Clickhouse.AuditRecord

import org.testng.Assert
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.descriptionColumn
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.implementation.clickhouse.model.DbColumn
import tanvd.audit.implementation.clickhouse.model.DbColumnType
import tanvd.audit.implementation.clickhouse.model.DbRow
import tanvd.audit.implementation.clickhouse.model.toDbColumnHeader
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
import utils.TestClassFirst
import utils.TestClassSecond
import utils.TypeUtils
import java.util.*

internal class AuditRecordInternalSerializationClickhouse {

    @Suppress("UNCHECKED_CAST")
    @BeforeClass
    fun initTypeSystem() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()
        AuditType.addType(AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>)
        AuditType.addType(AuditType(TestClassSecond::class, "TestClassSecond", TestClassSecond) as AuditType<Any>)
    }

    @Test
    fun serializeArray_PrimitiveTypesDifferent_serializedAsExpected() {
        val auditRecord = AuditRecordInternal(arrayListOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(Int::class), "123")), getSampleInformation())
        val row = ClickhouseRecordSerializer.serialize(auditRecord)

        val expectedSet = setOf(DbColumn("String", listOf("1234"), DbColumnType.DbArrayString),
                DbColumn("Int", listOf("123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("String", "Int"), DbColumnType.DbArrayString),
                *getSampleInformationColumns())
        Assert.assertEquals(row.columns.toSet(), expectedSet)
        Assert.assertEquals(row.columns.size, expectedSet.size)
    }

    @Test
    fun serializeArray_PrimitiveTypesCoincident_serializedAsExpected() {
        val auditRecord = AuditRecordInternal(arrayListOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(String::class), "123")), getSampleInformation())
        val row = ClickhouseRecordSerializer.serialize(auditRecord)

        val expectedSet = setOf(
                DbColumn("String", listOf("1234", "123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("String", "String"), DbColumnType.DbArrayString),
                *getSampleInformationColumns())
        Assert.assertEquals(row.columns.toSet(), expectedSet)
        Assert.assertEquals(row.columns.size, expectedSet.size)
    }

    @Test
    fun serializeArray_NonPrimitiveTypesDifferent_serializedAsExpected() {
        val auditRecord = AuditRecordInternal(arrayListOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassSecond::class), "123")), getSampleInformation())
        val row = ClickhouseRecordSerializer.serialize(auditRecord)

        val expectedSet = setOf(
                DbColumn("TestClassFirst", listOf("1234"), DbColumnType.DbArrayString),
                DbColumn("TestClassSecond", listOf("123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("TestClassFirst", "TestClassSecond"), DbColumnType.DbArrayString),
                *getSampleInformationColumns())
        Assert.assertEquals(row.columns.toSet(), expectedSet)
        Assert.assertEquals(row.columns.size, expectedSet.size)
    }

    @Test
    fun serializeArray_NonPrimitiveTypesCoincident_serializedAsExpected() {
        val auditRecord = AuditRecordInternal(arrayListOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassFirst::class), "123")), getSampleInformation())
        val row = ClickhouseRecordSerializer.serialize(auditRecord)

        val expectedSet = setOf(
                DbColumn("TestClassFirst", listOf("1234", "123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("TestClassFirst", "TestClassFirst"), DbColumnType.DbArrayString),
                *getSampleInformationColumns())
        Assert.assertEquals(row.columns.toSet(), expectedSet)
        Assert.assertEquals(row.columns.size, expectedSet.size)
    }

    @Test
    fun deserializeArray_PrimitiveTypesDifferent_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(descriptionColumn, arrayListOf("String", "Int"), DbColumnType.DbArrayString),
                DbColumn("String", arrayListOf("1234"), DbColumnType.DbArrayString),
                DbColumn("Int", arrayListOf("123"), DbColumnType.DbArrayString),
                *getSampleInformationColumns()
        )))


        Assert.assertEquals(auditRecord, AuditRecordInternal(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(Int::class), "123")), getSampleInformation()))
    }

    @Test
    fun deserializeArray_PrimitiveTypesCoincident_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(descriptionColumn, arrayListOf("String", "String"), DbColumnType.DbArrayString),
                DbColumn("String", arrayListOf("1234", "123"), DbColumnType.DbArrayString),
                *getSampleInformationColumns()
        )))
        Assert.assertEquals(auditRecord, AuditRecordInternal(listOf(
                Pair(AuditType.resolveType(String::class), "1234"),
                Pair(AuditType.resolveType(String::class), "123")), getSampleInformation()))
    }

    @Test
    fun deserializeArray_NonPrimitiveTypesDifferent_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(descriptionColumn, arrayListOf("TestClassFirst", "TestClassSecond"),
                        DbColumnType.DbArrayString),
                DbColumn("TestClassFirst", arrayListOf("1234"), DbColumnType.DbArrayString),
                DbColumn("TestClassSecond", arrayListOf("123"), DbColumnType.DbArrayString),
                *getSampleInformationColumns()
        )))

        Assert.assertEquals(auditRecord, AuditRecordInternal(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "1234"),
                Pair(AuditType.resolveType(TestClassSecond::class), "123")), getSampleInformation()))
    }

    @Test
    fun serializeDeserializeArray_NonPrimitiveTypes_deserializedAsExpected() {

        val arrayObjects = ArrayList(listOf(
                Pair(AuditType.resolveType(TestClassFirst::class), "TestClassFirstId"),
                Pair(AuditType.resolveType(String::class), "string"),
                Pair(AuditType.resolveType(Int::class), "27"),
                Pair(AuditType.resolveType(TestClassSecond::class), "TestClassSecondId")))

        val row = ClickhouseRecordSerializer.serialize(AuditRecordInternal(arrayObjects, getSampleInformation()))
        val deserializedRecord = ClickhouseRecordSerializer.deserialize(row)

        Assert.assertEquals(deserializedRecord, AuditRecordInternal(arrayObjects, getSampleInformation()))
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(0, 1, 2)
    }

    private fun getSampleInformationColumns(): Array<DbColumn> {
        return arrayOf(
                DbColumn(InformationType.resolveType(VersionPresenter).toDbColumnHeader(), listOf("2")),
                DbColumn(InformationType.resolveType(TimeStampPresenter).toDbColumnHeader(), listOf("1")),
                DbColumn(InformationType.resolveType(IdPresenter).toDbColumnHeader(), listOf("0"))
        )
    }
}
