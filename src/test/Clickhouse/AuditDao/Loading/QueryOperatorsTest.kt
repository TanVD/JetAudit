package Clickhouse.AuditDao.Loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.and
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.queries.or
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
import utils.TypeUtils

internal class QueryOperatorsTest {

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
    fun loadRows_AndStringsEqual_loadedBoth() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSampleArrayObjects("string1", "string2"), getSampleInformation())
        val auditRecordSecondOriginal = AuditRecordInternal(getSampleArrayObjects("string1", "string2"), getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_AndStringsEqual_loadedOne() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSampleArrayObjects("string1", "string2"), getSampleInformation())
        val auditRecordSecondOriginal = AuditRecordInternal(getSampleArrayObjects("string1", "string1"), getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_AndStringsEqual_loadedNone() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSampleArrayObjects("string1", "string1"), getSampleInformation())
        val auditRecordSecondOriginal = AuditRecordInternal(getSampleArrayObjects("string1", "string1"), getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRows_OrStringsEqual_loadedBoth() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSampleArrayObjects("string1", "string1"), getSampleInformation())
        val auditRecordSecondOriginal = AuditRecordInternal(getSampleArrayObjects("string2", "string2"), getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_OrStringsEqual_loadedOne() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSampleArrayObjects("string1", "string1"), getSampleInformation())
        val auditRecordSecondOriginal = AuditRecordInternal(getSampleArrayObjects("string3", "string3"), getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_OrStringsEqual_loadedNone() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSampleArrayObjects("string3", "string3"), getSampleInformation())
        val auditRecordSecondOriginal = AuditRecordInternal(getSampleArrayObjects("string4", "string4"), getSampleInformation())
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2)
    }

    private fun getSampleArrayObjects(stringFirst: String, stringSecond: String): List<Pair<AuditType<Any>, String>> {
        return arrayListOf(
                Pair(AuditType.resolveType(String::class), stringFirst),
                Pair(AuditType.resolveType(String::class), stringSecond))
    }

}
