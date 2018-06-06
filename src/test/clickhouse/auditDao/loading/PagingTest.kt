package clickhouse.auditDao.loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.aorm.query.Order
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.audit.model.external.equal
import tanvd.audit.model.external.limit
import tanvd.audit.model.external.orderBy
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.presenters.TimeStampType
import tanvd.audit.model.external.records.InformationObject
import utils.InformationUtils
import utils.SamplesGenerator
import utils.SamplesGenerator.getRecordInternal
import utils.TestUtil

internal class PagingTest {

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
    }

    @Test
    fun loadRows_limitOneFromZero_gotFirst() {
        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation(1))
        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation(2))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(TimeStampType to Order.ASC),
                limit(1, 0))

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_limitOneFromFirst_gotSecond() {
        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation(1))
        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation(2))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(TimeStampType to Order.ASC),
                limit(1, 1))

        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_limitTwoFromZero_gotBoth() {
        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation(1))
        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation(2))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(TimeStampType to Order.ASC),
                limit(2, 0))

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun countRows_countNoSavedRows_gotRightNumber() {
        val count = auditDao!!.countRecords(StringPresenter.value equal "string")
        Assert.assertEquals(count, 0)
    }

    @Test
    fun countRows_countTwoSavedRows_gotRightNumber() {
        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val count = auditDao!!.countRecords(StringPresenter.value equal "string")
        Assert.assertEquals(count, 2)
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(timeStamp: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2, SamplesGenerator.getMillenniumStart())
    }

}

