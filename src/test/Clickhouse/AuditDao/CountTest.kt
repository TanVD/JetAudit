package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.LongPresenter
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import utils.*

internal class CountTest {

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
    fun countRecord_primitiveTypes_countOne() {
        val auditRecordOriginal = SamplesGenerator.getRecordInternal(123L, "string", information = getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val numberRecords = auditDao!!.countRecords(LongPresenter.value equal 123)
        Assert.assertEquals(numberRecords, 1)
    }

    @Test
    fun saveRecords_PrimitiveTypes_countTwo() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(123L, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal(123L, "string1", information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val numberRecords = auditDao!!.countRecords(LongPresenter.value equal 123)
        Assert.assertEquals(numberRecords, 2)

    }

    @Test
    fun saveRecords_PrimitiveTypes_countZero() {
        val auditRecordFirstOriginal = SamplesGenerator.getRecordInternal(123L, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = SamplesGenerator.getRecordInternal(123L, "string1", information = getSampleInformation())

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val numberRecords = auditDao!!.countRecords(LongPresenter.value equal 256)
        Assert.assertEquals(numberRecords, 0)

    }

    @Test
    fun countRecords_deletedRecord_countOne() {
        val auditRecordFirst = SamplesGenerator.getRecordInternal(123L, "string", information = getSampleInformation(true))
        val auditRecordSecond = SamplesGenerator.getRecordInternal(123L, "string1", information = getSampleInformation(false))


        auditDao!!.saveRecords(listOf(auditRecordFirst, auditRecordSecond))

        val numberRecords = auditDao!!.countRecords(LongPresenter.value equal 123)
        Assert.assertEquals(numberRecords, 1)
    }

    private fun getSampleInformation(isDeleted: Boolean = false): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(CountTest.currentId++, 1, 2, SamplesGenerator.getMillenniumStart(), isDeleted)
    }
}