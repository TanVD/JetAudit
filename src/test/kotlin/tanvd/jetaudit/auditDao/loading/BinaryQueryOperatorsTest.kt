package tanvd.jetaudit.auditDao.loading

import org.junit.*
import tanvd.aorm.query.and
import tanvd.aorm.query.or
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.presenters.StringPresenter
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.utils.*
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal

internal class BinaryQueryOperatorsTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouse? = null
    }


    @Before
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = TestUtil.create()
    }

    @After
    fun clearAll() {
        TestUtil.drop()
        currentId = 0
    }

    @Test
    fun loadRows_AndStringsEqual_loadedBoth() {
        val auditRecordFirstOriginal = getRecordInternal("string1", "string2", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal("string1", "string2", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (StringPresenter.value equal "string1") and (StringPresenter.value equal "string2"))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_AndStringsEqual_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal("string1", "string2", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal("string1", "string1", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (StringPresenter.value equal "string1") and (StringPresenter.value equal "string2"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_AndStringsEqual_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal("string1", "string1", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal("string1", "string1", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (StringPresenter.value equal "string1") and (StringPresenter.value equal "string2"))
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRows_OrStringsEqual_loadedBoth() {
        val auditRecordFirstOriginal = getRecordInternal("string1", "string1", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal("string2", "string2", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (StringPresenter.value equal "string1") or (StringPresenter.value equal "string2"))
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_OrStringsEqual_loadedOne() {
        val auditRecordFirstOriginal = getRecordInternal("string1", "string1", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal("string3", "string3", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (StringPresenter.value equal "string1") or (StringPresenter.value equal "string2"))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_OrStringsEqual_loadedNone() {
        val auditRecordFirstOriginal = getRecordInternal("string3", "string3", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal("string4", "string4", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (StringPresenter.value equal "string1") or (StringPresenter.value equal "string2"))
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }
}
