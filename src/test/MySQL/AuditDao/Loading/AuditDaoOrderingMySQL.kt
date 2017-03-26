package MySQL.AuditDao.Loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.external.QueryParameters
import tanvd.audit.model.external.QueryParameters.OrderByParameters.Order.ASC
import tanvd.audit.model.external.QueryParameters.OrderByParameters.Order.DESC
import tanvd.audit.model.external.equal
import tanvd.audit.model.internal.AuditRecord
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer

/** Ordering in MySQL supported only for queried columns. */
internal class AuditDaoOrderingMySQL {

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
    fun loadRows_AscendingByTimeStamp_AscendingOrder() {
        val arrayObjectsFirst = arrayListOf(Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, ASC)
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    @Test
    fun loadRows_DescendingByTimeStamp_DescendingOrder() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 127)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 254)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, DESC)
        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", parameters)

        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal, auditRecordFirstOriginal))
    }
}
