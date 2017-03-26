package MySQL.AuditDao.Loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import tanvd.audit.model.external.*
import tanvd.audit.model.internal.AuditRecord
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer

/** Be aware that in case of MySQL you MUST NOT use multiple instances of one type in one expression. **/
internal class AuditDaoQueryOperatorsMySQL {

    companion object {
        var auditDao: AuditDaoMysqlImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = DbType.MySQL.getDao("jdbc:mysql://localhost/example?useLegacyDatetimeCode=false" +
                "&serverTimezone=Europe/Moscow", "root", "root") as AuditDaoMysqlImpl

        val typeString = AuditType(String::class, "Type_String", StringSerializer) as AuditType<Any>
        AuditType.addType(typeString)
        auditDao!!.addTypeInDbModel(typeString)

        val typeInt = AuditType(Int::class, "Type_Int", IntSerializer) as AuditType<Any>
        AuditType.addType(typeInt)
        auditDao!!.addTypeInDbModel(typeInt)
    }

    @AfterMethod
    fun clearAll() {
        auditDao!!.dropTable(AuditDaoMysqlImpl.auditTable)
        for (type in AuditType.getTypes()) {
            auditDao!!.dropTable(type.code)
        }
        AuditType.clearTypes()
    }

    @Test
    fun loadRows_AndStringsEqual_loadedBoth() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(Int::class), "123"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(Int::class), "123"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (Int::class equal 123), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_AndStringsEqual_loadedOne() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(Int::class), "123"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(Int::class), "456"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (Int::class equal 123), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_AndStringsEqual_loadedNone() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(Int::class), "456"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(Int::class), "456"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") and (Int::class equal 123), QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRows_OrStringsEqual_loadedBoth() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string1"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(Int::class), "456"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (Int::class equal 456), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

    }

    @Test
    fun loadRows_OrStringsEqual_loadedOne() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string1"),
                Pair(AuditType.resolveType(String::class), "string1"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(Int::class), "456"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (Int::class equal 123), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))

    }

    @Test
    fun loadRows_OrStringsEqual_loadedNone() {
        val arrayObjectsFirst = listOf(
                Pair(AuditType.resolveType(String::class), "string2"),
                Pair(AuditType.resolveType(String::class), "string2"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = listOf(
                Pair(AuditType.resolveType(Int::class), "456"),
                Pair(AuditType.resolveType(Int::class), "456"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (String::class equal "string1") or (Int::class equal 123), QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }
}
