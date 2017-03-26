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

internal class AuditDaoQueryTimeStampConditionsMySQL() {

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
    fun loadRow_LoadByLess_loadedOne() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(AuditUnixTimeStamp::class lessThan 129, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLess_loadedNone() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(AuditUnixTimeStamp::class lessThan 125, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByMore_loadedOne() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(AuditUnixTimeStamp::class moreThan 125, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByMore_loadedNone() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(AuditUnixTimeStamp::class moreThan 129, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByEqual_loadedOne() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(AuditUnixTimeStamp::class equalTo 127, QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(AuditUnixTimeStamp::class equalTo 129, QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByMoreAndLess_loadedTwo() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string1"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string2"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val recordsLoaded = auditDao!!.loadRecords(
                (AuditUnixTimeStamp::class moreThan 120) and (AuditUnixTimeStamp::class lessThan 260), QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

}

