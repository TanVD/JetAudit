package Clickhouse.AuditDao.Loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.presenters.TimeStampPresenter
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.QueryParameters.OrderByParameters.Order.ASC
import tanvd.audit.model.external.queries.QueryTypeCondition
import tanvd.audit.model.external.queries.QueryTypeLeaf
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
import utils.TypeUtils

internal class PagingTest {

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
    fun loadRows_limitOneFromZero_gotFirst() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation(1))
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation(2))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setLimits(0, 1)
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, ASC)))
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_limitOneFromFirst_gotSecond() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation(1))
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation(2))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setLimits(1, 1)
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, ASC)))
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)

        Assert.assertEquals(recordsLoaded.size, 1)
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_limitTwoFromZero_gotBoth() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation(1))
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation(2))
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setLimits(0, 2)
        parameters.setOrder(information = listOf(Pair(TimeStampPresenter, ASC)))
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun countRows_countNoSavedRows_gotRightNumber() {
        val count = auditDao!!.countRecords(QueryTypeLeaf(QueryTypeCondition.equal, "string", String::class))
        Assert.assertEquals(count, 0)
    }

    @Test
    fun countRows_countTwoSavedRows_gotRightNumber() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecordInternal(arrayObjectsFirst, getSampleInformation())
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val count = auditDao!!.countRecords(String::class equal "string")
        Assert.assertEquals(count, 2)
    }

    private fun getSampleInformation(): Set<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2)
    }

    private fun getSampleInformation(timeStamp: Long): Set<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, timeStamp, 2)
    }

}

