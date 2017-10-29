package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.model.external.presenters.IdType
import tanvd.audit.model.external.presenters.TimeStampType
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import utils.*
import utils.SamplesGenerator.getRecordInternal

internal class AddInformationTypeTest {


    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
        var currentId = 0L

    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = TestUtil.create()
    }

    @AfterMethod
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    @Test
    fun saveRecordBefore_addType_loadRecordsReturnSavedRecord() {
        val auditRecordOriginal = getRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordOriginal)

        addStringInformation()

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType equal 1)

        auditRecordOriginal.information.add(InformationObject(StringInf.default(),
                StringInf))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNotNew() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = getRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(IdType equal 0)

        auditRecordFirstOriginal.information.add(InformationObject(StringInf.default(),
                StringInf))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNew() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = getRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(StringInf equal "string")

        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedBoth() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        addStringInformation()

        val auditRecordSecondOriginal = getRecordInternal(information = getSampleWithStringInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType equal 1)

        auditRecordFirstOriginal.information.add(InformationObject(StringInf.default(), StringInf))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleWithStringInformation(): LinkedHashSet<InformationObject<*>> {
        val information = getSampleInformation()
        information.add(InformationObject("string", InformationType.resolveType("StringInfColumn")))
        return information
    }

    private fun addStringInformation() {
        @Suppress("UNCHECKED_CAST")
        InformationType.addType(StringInf)
        auditDao!!.addInformationInDbModel(StringInf)
    }
}

