package Clickhouse.AuditDao.Loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.*
import tanvd.audit.model.internal.AuditRecord
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer

internal class AuditDaoPagingClickhouse() {

    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = DbType.Clickhouse.getDao("jdbc:clickhouse://localhost:8123/example", "default", "") as AuditDaoClickhouseImpl

        val typeString = AuditType(String::class, "String", StringSerializer) as AuditType<Any>
        AuditType.addType(typeString)
        auditDao!!.addTypeInDbModel(typeString)

        val typeInt = AuditType(Int::class, "Int", IntSerializer) as AuditType<Any>
        AuditType.addType(typeInt)
        auditDao!!.addTypeInDbModel(typeInt)

    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoClickhouseImpl.auditTable)
        AuditType.clearTypes()
    }

    @Test
    fun loadRows_limitOneFromZero_gotFirst() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setLimits(0, 1)
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRows_limitOneFromFirst_gotSecond() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setLimits(1, 1)
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)

        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_limitTwoFromZero_gotBoth() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "123"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setLimits(0, 2)
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
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val count = auditDao!!.countRecords(String::class equal "string")
        Assert.assertEquals(count, 2)
    }
}

