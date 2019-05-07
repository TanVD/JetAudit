package tanvd.jetaudit.auditDao.loading.information

import org.junit.*
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.*
import tanvd.jetaudit.model.external.presenters.DateType
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.utils.*

internal class InformationDateQueriesTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouse? = null
    }

    @Before
    fun createAll() {
        auditDao = TestUtil.create()
    }

    @After
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    //Equality
    @Test
    fun loadRow_LoadByEqual_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType equal getDate("2000-01-01"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType equal getDate("2000-01-02"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    //Number
    @Test
    fun loadRow_LoadByLess_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType less getDate("2000-01-02"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLess_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType less getDate("2000-01-01"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    @Test
    fun loadRow_LoadByMore_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType more getDate("1999-01-01"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByMore_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType more getDate("2000-01-01"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    @Test
    fun loadRow_LoadByLessOrEqual_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType lessOrEq getDate("2000-01-01"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLessOrEqual_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType lessOrEq getDate("1999-01-01"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    @Test
    fun loadRow_LoadByMoreOrEqual_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType moreOrEq getDate("2000-01-01"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByMoreOrEqual_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType moreOrEq getDate("2000-01-02"))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }

    //List
    @Test
    fun loadRow_LoadByInList_loadedOne() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType inList listOf(getDate("2000-01-01")))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByInList_loadedNone() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(information = getSampleInformation("2000-01-01"))

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(DateType inList listOf(getDate("2000-01-02")))
        Assert.assertTrue(recordsLoaded.isEmpty())
    }


    private fun getSampleInformation(dateStamp: String): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 100, 2, getDate(dateStamp))
    }
}
