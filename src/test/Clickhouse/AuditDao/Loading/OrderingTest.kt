package Clickhouse.AuditDao.Loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order.ASC
import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order.DESC
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.queries.or
import tanvd.audit.model.external.records.InformationObject
import utils.DbUtils
import utils.InformationUtils
import utils.SamplesGenerator
import utils.SamplesGenerator.getRecordInternal
import utils.TypeUtils

internal class OrderingTest {

    companion object {
        var currentId = 0L
        var auditDao: AuditDaoClickhouseImpl? = null
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
    fun loadRecords_AscendingByIntIds_AscendingOrder() {
        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setObjectStatesOrder(IntPresenter.value to ASC)


        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_DescendingByIntsIds_DescendingOrder() {
        val auditRecordFirstOriginal = getRecordInternal(123, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(456, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setObjectStatesOrder(IntPresenter.value to DESC)
        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))

    }

    @Test
    fun loadRecords_AscendingByTimestamp_AscendingOrder() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(3))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setInformationOrder(TimeStampPresenter to ASC)

        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_DescendingByTimestamp_DescendingOrder() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(3))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setInformationOrder(TimeStampPresenter to DESC)

        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_AscendingIntsIdsArraysFirstElementDifferent_AscendingByFirstElement() {
        val auditRecordFirstOriginal = getRecordInternal(254, 127, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(127, 254, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setObjectStatesOrder(IntPresenter.value to ASC)
        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_DescendingIntsIdsArraysFirstElementDifferent_DescendingByFirstElement() {
        val auditRecordFirstOriginal = getRecordInternal(254, 127, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(127, 254, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setObjectStatesOrder(IntPresenter.value to DESC)
        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_AscendingIntsIdsArraysFirstElementEqual_AscendingBySecondElement() {
        val auditRecordFirstOriginal = getRecordInternal(127, 254, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(127, 508, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setObjectStatesOrder(IntPresenter.value to ASC)
        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_DescendingIntsIdsArraysFirstElementEqual_DescendingBySecondElement() {
        val auditRecordFirstOriginal = getRecordInternal(254, 127, "string", information = getSampleInformation())
        val auditRecordSecondOriginal = getRecordInternal(254, 508, "string", information = getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setObjectStatesOrder(IntPresenter.value to DESC)
        val recordsLoaded = auditDao!!.loadRecords(StringPresenter.value equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_AscendingTimestampVersionArraysFirstElementDifferent_AscendingByFirstElement() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2, 3))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(3, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setInformationOrder(TimeStampPresenter to ASC, VersionPresenter to DESC)
        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_DescendingTimestampVersionArraysFirstElementDifferent_DescendingByFirstElement() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2, 3))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(3, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setInformationOrder(TimeStampPresenter to DESC, VersionPresenter to ASC)
        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_AscendingTimestampVersionArraysFirstElementEqual_AscendingBySecondElement() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2, 3))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(2, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setInformationOrder(TimeStampPresenter to DESC, VersionPresenter to ASC)
        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_DescendingTimestampVersionArraysFirstElementEqual_DescendingBySecondElement() {
        val auditRecordFirstOriginal = getRecordInternal(information = getSampleInformation(2, 3))
        val auditRecordSecondOriginal = getRecordInternal(information = getSampleInformation(2, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setInformationOrder(TimeStampPresenter to ASC, VersionPresenter to DESC)
        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    private fun getSampleInformation(): MutableSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(timeStamp: Long): MutableSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2, SamplesGenerator.getMillenniumStart())
    }

    private fun getSampleInformation(timeStamp: Long, version: Long): MutableSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, version, SamplesGenerator.getMillenniumStart())
    }


}
