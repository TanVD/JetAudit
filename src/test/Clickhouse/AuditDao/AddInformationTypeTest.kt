package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
import utils.StringPresenter
import utils.TypeUtils

internal class AddInformationTypeTest {


    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
        var currentId = 0L

    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

        TypeUtils.addAuditTypePrimitive(auditDao!!)
    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
        currentId = 0
    }

    @Test
    fun saveRecordBefore_addType_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = AuditRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordOriginal)

        addStringInformation()

        val recordsLoaded = auditDao!!.loadRecords(TimeStampPresenter equal 1, QueryParameters())

        auditRecordOriginal.information.add(InformationObject(StringPresenter.getDefault(),
                InformationType.resolveType(StringPresenter)))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNotNew() {
        val auditRecordFirstOriginal = AuditRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = AuditRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(IdPresenter equal 0, QueryParameters())

        auditRecordFirstOriginal.information.add(InformationObject(StringPresenter.getDefault(),
                InformationType.resolveType(StringPresenter)))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNew() {
        val auditRecordFirstOriginal = AuditRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = AuditRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter equal "string", QueryParameters())

        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedBoth() {
        val auditRecordFirstOriginal = AuditRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = AuditRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TimeStampPresenter equal 1, QueryParameters())

        auditRecordFirstOriginal.information.add(InformationObject(StringPresenter.getDefault(),
                InformationType.resolveType(StringPresenter)))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2)
    }

    private fun getSampleWithStringInformation(): MutableSet<InformationObject> {
        val information = getSampleInformation()
        information.add(InformationObject("string", InformationType.resolveType("OneStringField")))
        return information
    }

    private fun addStringInformation() {
        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringPresenter, "OneStringField", InformationType.InformationInnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)
    }
}

