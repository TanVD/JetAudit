package Clickhouse.AuditDao.Loading.Information

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.queries.like
import tanvd.audit.model.external.queries.regexp
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
import utils.StringPresenter
import utils.TypeUtils

internal class QueryStringInformationConditionTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

        TypeUtils.addAuditTypePrimitive(auditDao!!)

        val type = InformationType(StringPresenter, "StringPresenter", InformationType.InformationInnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)
    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
        currentId = 0
    }

    @Test
    fun loadRow_LoadByEqual_loadedOne() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter equal "error", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByLike_loadedOne() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter like "str%", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIsNot_loadedNone() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter like "s_", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByRegExp_loadedOne() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter regexp "st", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByRegExp_loadedNone() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter regexp "zo", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(value: String): MutableSet<InformationObject> {
        val information = InformationUtils.getPrimitiveInformation(currentId++, 1, 2)
        information.add(InformationObject(value, StringPresenter))
        return information

    }
}
