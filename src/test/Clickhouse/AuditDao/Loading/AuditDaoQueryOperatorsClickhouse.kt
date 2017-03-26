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

internal class AuditDaoQueryOperatorsClickhouse() {

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
    fun loadRows_AndStringsEqual_loadedBoth() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string2"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string2"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_AndStringsEqual_loadedOne() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string2"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string1"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_AndStringsEqual_loadedNone() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string1"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string1"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRows_OrStringsEqual_loadedBoth() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string1"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string2"),
                Pair(AuditType.resolveType(String::class), "string2"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_OrStringsEqual_loadedOne() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string1"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string3"),
                Pair(AuditType.resolveType(String::class), "string3"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_OrStringsEqual_loadedNone() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string3"),
                Pair(AuditType.resolveType(String::class), "string3"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string4"),
                Pair(AuditType.resolveType(String::class), "string4"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (String::class equal "string2"), QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

}
