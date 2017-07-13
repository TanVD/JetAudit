package Clickhouse.AuditDao

import Clickhouse.AuditDao.Loading.Information.InformationBoolean
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.objects.ObjectType
import utils.*
import utils.SamplesGenerator.getRecordInternal

internal class AddObjectTypeTest {


    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
        var currentId = 0L

    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        AuditDao.credentials = DbUtils.getCredentials()
        auditDao = AuditDao.getDao() as AuditDaoClickhouseImpl

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
        val auditRecordOriginal = getRecordInternal("string", information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordOriginal)

        addTestClassStringType()

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNotNew() {
        val auditRecordFirstOriginal = getRecordInternal("string", 123, information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addTestClassStringType()

        val auditRecordSecondOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", QueryParameters())

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNew() {
        val auditRecordFirstOriginal = getRecordInternal("string", 123, information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addTestClassStringType()

        val auditRecordSecondOriginal = getRecordInternal(TestClassString("string"), information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)


        val recordsLoaded = auditDao!!.loadRecords(TestClassStringPresenter.id equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedBoth() {
        val auditRecordFirstOriginal = getRecordInternal("string", 123, information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addTestClassStringType()

        val auditRecordSecondOriginal = getRecordInternal("string", TestClassString("string"), information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)


        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    private fun addTestClassStringType() {
        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = ObjectType(TestClassString::class, TestClassStringPresenter) as ObjectType<Any>
        ObjectType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }
}
