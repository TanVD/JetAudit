package clickhouse.auditDao.loading.information

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.audit.model.external.equal
import tanvd.audit.model.external.inList
import tanvd.audit.model.external.less
import tanvd.audit.model.external.more
import tanvd.audit.model.external.presenters.TimeStampType
import tanvd.audit.model.external.records.InformationObject
import utils.InformationUtils
import utils.SamplesGenerator
import utils.SamplesGenerator.getRecordInternal
import utils.TestUtil

internal class InformationLongQueriesTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouse? = null
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

    //Equality
    @Test
    fun loadRow_LoadByEqual_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType equal 1)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType equal 0)
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    //Number
    @Test
    fun loadRow_LoadByLess_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType less 2)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLess_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType less 1)
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByMore_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType more 0)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByMore_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType more 1)
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    //List
    @Test
    fun loadRow_LoadByInList_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType inList listOf(1L))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByInList_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(1))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(TimeStampType inList listOf(0L))
        Assert.assertEquals(recordsLoaded.size, 0)
    }


    private fun getSampleInformation(timeStamp: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2, SamplesGenerator.getMillenniumStart())
    }
}

