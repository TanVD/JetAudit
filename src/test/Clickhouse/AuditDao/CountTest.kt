package Clickhouse.AuditDao

import Clickhouse.AuditDao.Saving.SavingTest
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.LongPresenter
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import utils.DbUtils
import utils.InformationUtils
import utils.SamplesGenerator
import utils.TypeUtils

internal class SavingTest {

    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
        var currentId = 0L
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {

        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        AuditDao.credentials = DbUtils.getCredentials()
        auditDao = AuditDao.getDao() as AuditDaoClickhouseImpl

        TypeUtils.addAuditTypePrimitive(auditDao!!)
    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        TypeUtils.clearTypes()
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

    private fun getSampleInformation(): MutableSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(SavingTest.currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }
}