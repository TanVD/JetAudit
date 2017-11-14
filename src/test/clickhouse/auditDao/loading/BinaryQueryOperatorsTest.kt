package clickhouse.auditDao.loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.aorm.query.and
import tanvd.aorm.query.or
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import utils.InformationUtils
import utils.SamplesGenerator
import utils.SamplesGenerator.getRecordInternal
import utils.TestUtil

internal class BinaryQueryOperatorsTest {

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
