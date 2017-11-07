package Clickhouse.AuditDao.Loading.Information

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.model.external.queries.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import utils.*
import utils.getDate
import utils.getDateTime

internal class InformationDateTimeQueriesTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = TestUtil.create()
        InformationType.addType(DateTimeInf)
        auditDao!!.addInformationInDbModel(DateTimeInf)
    }

    @AfterMethod
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    //Equality
    @Test
    fun loadRow_LoadByEqual_loadedOne() {
        val dateTime = "2000-01-01 12:00:00"
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation(dateTime))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf equal getDateTime(dateTime))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf equal getDateTime("2000-01-01 13:00:00"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    //Number
    @Test
    fun loadRow_LoadByLess_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf less getDateTime("2000-01-01 13:00:00"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLess_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf less getDateTime("2000-01-01 12:00:00"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    @Test
    fun loadRow_LoadByMore_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf more getDateTime("2000-01-01 11:00:00"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByMore_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf more getDateTime("2000-01-01 12:00:00"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    @Test
    fun loadRow_LoadByLessOrEqual_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf lessOrEq getDateTime("2000-01-01 12:00:00"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLessOrEqual_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf lessOrEq getDateTime("2000-01-01 11:00:00"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    @Test
    fun loadRow_LoadByMoreOrEqual_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf moreOrEq getDateTime("2000-01-01 12:00:00"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByMoreOrEqual_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf more getDateTime("2000-01-01 13:00:00"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    //List
    @Test
    fun loadRow_LoadByInList_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf inList listOf(getDateTime("2000-01-01 12:00:00")))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByInList_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01 12:00:00"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateTimeInf inList listOf(getDateTime("2000-01-01 13:00:00")))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }


    private fun getSampleInformation(dateTime: String = "2000-01-01 12:00:00"): LinkedHashSet<InformationObject<*>> {
        val set =  InformationUtils.getPrimitiveInformation(currentId++, 100, 2, getDate("2000-01-01"))
        set.add(InformationObject(getDateTime(dateTime), DateTimeInf))
        return set
    }
}