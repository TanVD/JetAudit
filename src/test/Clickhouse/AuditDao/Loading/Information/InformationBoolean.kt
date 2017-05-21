package Clickhouse.AuditDao.Loading.Information

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.model.external.db.DbType
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.`is`
import tanvd.audit.model.external.queries.isNot
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationType
import utils.*
import utils.SamplesGenerator.getRecordInternal

internal class InformationBoolean {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        auditDao = DbType.Clickhouse.getDao(DbUtils.getDbProperties()) as AuditDaoClickhouseImpl

        TypeUtils.addAuditTypePrimitive(auditDao!!)

        val type = InformationType(BooleanInfPresenter, InnerType.Boolean) as InformationType<Any>
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
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInfPresenter `is` true, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIs_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInfPresenter `is` false, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByIsNot_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInfPresenter isNot false, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIsNot_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(true))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(BooleanInfPresenter isNot true, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(value: Boolean): MutableSet<InformationObject> {
        val information = InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMilleniumnStart())
        (information).add(InformationObject(value, BooleanInfPresenter))
        return information

    }
}
