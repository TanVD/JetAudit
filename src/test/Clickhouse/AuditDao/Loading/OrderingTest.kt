package Clickhouse.AuditDao.Loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.presenters.IdPresenter
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.presenters.VersionPresenter
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order.ASC
import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order.DESC
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.queries.or
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
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

        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

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
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation())
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(Pair(Int::class, ASC), information = emptyList())


        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_DescendingByIntsIds_DescendingOrder() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation())
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(Pair(Int::class, DESC), information = emptyList())
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))

    }

    @Test
    fun loadRecords_AscendingByTimestamp_AscendingOrder() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), getSampleInformation(3))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, ASC)))

        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_DescendingByTimestamp_DescendingOrder() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), getSampleInformation(3))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, DESC)))

        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_AscendingIntsIdsArraysFirstElementDifferent_AscendingByFirstElement() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "254"),
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation())
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "254"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(Pair(Int::class, ASC), information = emptyList())
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_DescendingIntsIdsArraysFirstElementDifferent_DescendingByFirstElement() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "254"),
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation())
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "254"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(Pair(Int::class, DESC), information = emptyList())
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_AscendingIntsIdsArraysFirstElementEqual_AscendingBySecondElement() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "254"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation())
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(Int::class), "508"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(Pair(Int::class, ASC), information = emptyList())
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_DescendingIntsIdsArraysFirstElementEqual_DescendingBySecondElement() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "254"),
                Pair(AuditType.resolveType(Int::class), "127"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation())
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "254"),
                Pair(AuditType.resolveType(Int::class), "508"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(Pair(Int::class, DESC), information = emptyList())
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_AscendingTimestampVersionArraysFirstElementDifferent_AscendingByFirstElement() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2, 3))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), getSampleInformation(3, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, ASC), Pair(VersionPresenter, DESC)))
        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_DescendingTimestampVersionArraysFirstElementDifferent_DescendingByFirstElement() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2, 3))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), getSampleInformation(3, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, DESC), Pair(VersionPresenter, ASC)))
        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_AscendingTimestampVersionArraysFirstElementEqual_AscendingBySecondElement() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2, 3))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, DESC), Pair(VersionPresenter, ASC)))
        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_DescendingTimestampVersionArraysFirstElementEqual_DescendingBySecondElement() {
        val auditRecordFirstOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2, 3))
        val auditRecordSecondOriginal = AuditRecordInternal(emptyList(), getSampleInformation(2, 4))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, ASC), Pair(VersionPresenter, DESC)))
        val recordsLoaded = auditDao!!.loadRecords((IdPresenter equal 0) or (IdPresenter equal 1), parameters)
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }

    private fun getSampleInformation(): Set<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2)
    }

    private fun getSampleInformation(timeStamp: Long): Set<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2)
    }

    private fun getSampleInformation(timeStamp: Long, version: Long): Set<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, version)
    }


}
