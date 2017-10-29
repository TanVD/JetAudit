package Clickhouse.AuditDao.Loading.Information

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.*
import utils.SamplesGenerator.getRecordInternal

internal class InformationStringQueriesTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = TestUtil.create()

        InformationType.addType(StringInf)
        auditDao!!.addInformationInDbModel(StringInf)
    }

    @AfterMethod
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    //Equality
    @Test
    fun loadRow_LoadByEqual_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf equal "string")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf equal "error")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    //String
    @Test
    fun loadRow_LoadByLike_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf like "str%")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByIsNot_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf like "s_")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByRegExp_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf regex "st")
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByRegExp_loadedNone() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf regex "zo")
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    //List
    @Test
    fun loadRow_LoadByInList_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf inList listOf("string"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByInList_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation("string"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringInf inList listOf("error"))
        Assert.assertEquals(recordsLoaded.size, 0)
    }


    private fun getSampleInformation(value: String): MutableSet<InformationObject<*>> {
        val information = InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
        information.add(InformationObject(value, StringInf))
        return information

    }
}
