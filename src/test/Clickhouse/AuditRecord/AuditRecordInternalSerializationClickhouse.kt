package Clickhouse.AuditRecord

import org.testng.Assert
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl.Scheme.descriptionColumn
import tanvd.audit.implementation.clickhouse.ClickhouseRecordSerializer
import tanvd.audit.implementation.clickhouse.model.*
import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import utils.*
import utils.SamplesGenerator.getRecordInternal

internal class AuditRecordInternalSerializationClickhouse {

    @Suppress("UNCHECKED_CAST")
    @BeforeClass
    fun initTypeSystem() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()
        ObjectType.addType(ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>)
        ObjectType.addType(ObjectType(TestClassLong::class, TestClassLongPresenter) as ObjectType<Any>)
    }

    @Test
    fun serializeArray_PrimitiveTypesDifferent_serializedAsExpected() {
        val auditRecord = getRecordInternal("1234", 123, information = getSampleInformation())
        val row = ClickhouseRecordSerializer.serialize(auditRecord)

        val expectedSet = setOf(DbColumn(StringPresenter.value.getCode(), listOf("1234"), DbColumnType.DbArrayString),
                DbColumn(IntPresenter.value.getCode(), listOf("123"), DbColumnType.DbArrayLong),
                DbColumn(descriptionColumn, listOf(StringPresenter.entityName, IntPresenter.entityName), DbColumnType.DbArrayString),
                *getSampleInformationColumns())
        Assert.assertEquals(row.columns.toSet(), expectedSet)
        Assert.assertEquals(row.columns.size, expectedSet.size)
    }

    @Test
    fun serializeArray_PrimitiveTypesCoincident_serializedAsExpected() {
        val auditRecord = getRecordInternal("1234", "123", information = getSampleInformation())
        val row = ClickhouseRecordSerializer.serialize(auditRecord)

        val expectedSet = setOf(
                DbColumn(StringPresenter.value.getCode(), listOf("1234", "123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("String", "String"), DbColumnType.DbArrayString),
                *getSampleInformationColumns())
        Assert.assertEquals(row.columns.toSet(), expectedSet)
        Assert.assertEquals(row.columns.size, expectedSet.size)
    }

    @Test
    fun serializeArray_NonPrimitiveTypesDifferent_serializedAsExpected() {
        val auditRecord = getRecordInternal(TestClassString("123"), TestClassLong(1234), information = getSampleInformation())
        val row = ClickhouseRecordSerializer.serialize(auditRecord)

        val expectedSet = setOf(
                DbColumn(TestClassLongPresenter.id.getCode(), listOf("1234"), DbColumnType.DbArrayLong),
                DbColumn(TestClassStringPresenter.id.getCode(), listOf("123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("TestClassString", "TestClassLong"), DbColumnType.DbArrayString),
                *getSampleInformationColumns())
        Assert.assertEquals(row.columns.toSet(), expectedSet)
        Assert.assertEquals(row.columns.size, expectedSet.size)
    }

    @Test
    fun serializeArray_NonPrimitiveTypesCoincident_serializedAsExpected() {
        val auditRecord = getRecordInternal(TestClassString("1234"), TestClassString("123"), information = getSampleInformation())
        val row = ClickhouseRecordSerializer.serialize(auditRecord)

        val expectedSet = setOf(
                DbColumn(TestClassStringPresenter.id.getCode(), listOf("1234", "123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("TestClassString", "TestClassString"), DbColumnType.DbArrayString),
                *getSampleInformationColumns())
        Assert.assertEquals(row.columns.toSet(), expectedSet)
        Assert.assertEquals(row.columns.size, expectedSet.size)
    }

    @Test
    fun deserializeArray_PrimitiveTypesDifferent_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(descriptionColumn, arrayListOf("String", "Int"), DbColumnType.DbArrayString),
                DbColumn(StringPresenter.value.getCode(), arrayListOf("1234"), DbColumnType.DbArrayString),
                DbColumn(IntPresenter.value.getCode(), arrayListOf("123"), DbColumnType.DbArrayLong),
                *getSampleInformationColumns()
        )))


        Assert.assertEquals(auditRecord, getRecordInternal("1234", 123, information = getSampleInformation()))
    }

    @Test
    fun deserializeArray_PrimitiveTypesCoincident_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(StringPresenter.value.getCode(), listOf("1234", "123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("String", "String"), DbColumnType.DbArrayString),
                *getSampleInformationColumns()
        )))
        Assert.assertEquals(auditRecord, getRecordInternal("1234", "123", information = getSampleInformation()))
    }

    @Test
    fun deserializeArray_NonPrimitiveTypesDifferent_deserializedAsExpected() {
        val auditRecord = ClickhouseRecordSerializer.deserialize(DbRow(arrayListOf(
                DbColumn(TestClassLongPresenter.id.getCode(), listOf("1234"), DbColumnType.DbArrayLong),
                DbColumn(TestClassStringPresenter.id.getCode(), listOf("123"), DbColumnType.DbArrayString),
                DbColumn(descriptionColumn, listOf("TestClassString", "TestClassLong"), DbColumnType.DbArrayString),
                *getSampleInformationColumns()
        )))

        Assert.assertEquals(auditRecord, getRecordInternal(TestClassString("123"), TestClassLong(1234), information = getSampleInformation()))
    }

    @Test
    fun serializeDeserializeArray_NonPrimitiveTypes_deserializedAsExpected() {

        val record = getRecordInternal(TestClassString("string"), TestClassLong(15),
                27, "string", information = getSampleInformation())

        val row = ClickhouseRecordSerializer.serialize(record)
        val deserializedRecord = ClickhouseRecordSerializer.deserialize(row)

        Assert.assertEquals(deserializedRecord, record)
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(0, 1, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformationColumns(): Array<DbColumn> {
        return arrayOf(
                DbColumn(InformationType.resolveType(DatePresenter).toDbColumnHeader(), listOf("2000-01-01")),
                DbColumn(InformationType.resolveType(VersionPresenter).toDbColumnHeader(), listOf("2")),
                DbColumn(InformationType.resolveType(TimeStampPresenter).toDbColumnHeader(), listOf("1")),
                DbColumn(InformationType.resolveType(IdPresenter).toDbColumnHeader(), listOf("0"))
        )
    }
}
