package MySQL.AuditDao.Loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.external.QueryParameters
import tanvd.audit.model.external.equal
import tanvd.audit.model.external.like
import tanvd.audit.model.internal.AuditRecord
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer

internal class AuditDaoQueryTypeConditionsMySQL() {

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
    fun loadRow_LoadByEqual_loadedOne() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"),
                Pair(AuditType.resolveType(String::class), "BigString"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByEqual_loadedNone() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"),
                Pair(AuditType.resolveType(String::class), "BigString"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(String::class equal "Bad String", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }

    @Test
    fun loadRow_LoadByLike_loadedOne() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"),
                Pair(AuditType.resolveType(String::class), "BigString"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(String::class like ".*[stri].*", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun loadRow_LoadByLike_loadedNone() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"),
                Pair(AuditType.resolveType(String::class), "BigString"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)

        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal))

        val recordsLoaded = auditDao!!.loadRecords(String::class equal ".", QueryParameters())
        Assert.assertEquals(recordsLoaded.size, 0)
    }
}

