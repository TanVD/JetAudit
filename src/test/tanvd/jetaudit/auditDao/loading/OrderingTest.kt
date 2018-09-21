package tanvd.jetaudit.auditDao.loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.aorm.query.Order
import tanvd.aorm.query.or
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.orderBy
import tanvd.jetaudit.model.external.presenters.*
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.utils.InformationUtils
import tanvd.jetaudit.utils.SamplesGenerator
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal
import tanvd.jetaudit.utils.TestUtil

internal class OrderingTest {

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
    fun loadRecords_AscendingByIntIds_AscendingOrder() {
        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))


        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(IntPresenter.value to Order.ASC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_DescendingByIntsIds_DescendingOrder() {
        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(IntPresenter.value to Order.DESC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRecords_AscendingByTimestamp_AscendingOrder() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(3))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords((IdType equal 0) or (IdType equal 1), orderBy(TimeStampType to Order.ASC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_DescendingByTimestamp_DescendingOrder() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(3))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords((IdType equal 0) or (IdType equal 1), orderBy(TimeStampType to Order.DESC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_AscendingIntsIdsArraysFirstElementDifferent_AscendingByFirstElement() {
        val auditRecordFirstOriginal = getRecordInternal(254, 127, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(127, 254, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(IntPresenter.value to Order.ASC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_DescendingIntsIdsArraysFirstElementDifferent_DescendingByFirstElement() {
        val auditRecordFirstOriginal = getRecordInternal(254, 127, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(127, 254, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(IntPresenter.value to Order.DESC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_AscendingIntsIdsArraysFirstElementEqual_AscendingBySecondElement() {
        val auditRecordFirstOriginal = getRecordInternal(127, 254, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(127, 508, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(IntPresenter.value to Order.ASC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_DescendingIntsIdsArraysFirstElementEqual_DescendingBySecondElement() {
        val auditRecordFirstOriginal = getRecordInternal(254, 127, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(254, 508, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", orderBy(IntPresenter.value to Order.DESC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_AscendingTimestampVersionArraysFirstElementDifferent_AscendingByFirstElement() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2, 3))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(3, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords((IdType equal 0) or (IdType equal 1), orderBy(TimeStampType to Order.ASC,
                VersionType to Order.DESC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_DescendingTimestampVersionArraysFirstElementDifferent_DescendingByFirstElement() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2, 3))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(3, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords((IdType equal 0) or (IdType equal 1), orderBy(TimeStampType to Order.DESC,
                VersionType to Order.ASC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_AscendingTimestampVersionArraysFirstElementEqual_AscendingBySecondElement() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2, 3))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(2, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords((IdType equal 0) or (IdType equal 1), orderBy(TimeStampType to Order.DESC,
                VersionType to Order.ASC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_DescendingTimestampVersionArraysFirstElementEqual_DescendingBySecondElement() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2, 3))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(2, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords((IdType equal 0) or (IdType equal 1), orderBy(TimeStampType to Order.ASC,
                VersionType to Order.DESC))
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(timeStamp: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(timeStamp: Long, version: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, version, SamplesGenerator.getMillenniumStart())
    }


}
