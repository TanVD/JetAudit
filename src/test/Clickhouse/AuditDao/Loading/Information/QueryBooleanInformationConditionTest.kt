package Clickhouse.AuditDao.Loading.Information

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.`is`
import tanvd.audit.model.external.queries.isNot
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.BooleanPresenter
import utils.InformationUtils
import utils.TypeUtils

internal class QueryBooleanInformationConditionTest {

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

        val type = InformationType(BooleanPresenter, "BooleanPresenter", InformationType.InformationInnerType.Boolean) as InformationType<Any>
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
    fun loadRow_LoadByIs_loadedOne() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanPresenter `is` true, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIs_loadedNone() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanPresenter `is` false, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByIsNot_loadedOne() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanPresenter isNot false, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIsNot_loadedNone() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanPresenter isNot true, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(value: Boolean): Set<InformationObject> {
        val information = InformationUtils.getPrimitiveInformation(currentId++, 1, 2)
        (information).add(InformationObject(value, BooleanPresenter))
        return information

    }
}
