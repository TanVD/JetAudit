package MySQL.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import tanvd.audit.model.QueryParameters
import tanvd.audit.model.QueryParameters.OrderByParameters.Order.ASC
import tanvd.audit.model.QueryParameters.OrderByParameters.Order.DESC
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
        auditDao!!.dropTable("Audit")
        for (type in AuditType.getTypes()) {
            auditDao!!.dropTable(type.code)
        }
        AuditType.clearTypes()
    }

    @Test
    fun loadRows_AscendingByTimeStamp_AscendingOrder() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 127)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, ASC)
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)

    }

    @Test
    fun loadRows_DescendingByTimeStamp_DescendingOrder() {
        val arrayObjectsFirst = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordFirstOriginal = AuditRecord(arrayObjectsFirst, 25)
        val arrayObjectsSecond = arrayListOf(
                Pair(AuditType.resolveType(String::class), "string"))
        val auditRecordSecondOriginal = AuditRecord(arrayObjectsSecond, 127)
        auditDao!!.saveRecords(listOf(auditRecordFirstOriginal, auditRecordSecondOriginal))

        val parameters = QueryParameters()
        parameters.setOrder(true, DESC)
        val recordsLoaded = auditDao!!.loadRecords(AuditType.resolveType(String::class), "string", parameters)
        Assert.assertEquals(recordsLoaded.size, 2)
        Assert.assertEquals(recordsLoaded[0].objects, auditRecordSecondOriginal.objects)
        Assert.assertEquals(recordsLoaded[0].unixTimeStamp, auditRecordSecondOriginal.unixTimeStamp)
        Assert.assertEquals(recordsLoaded[1].objects, auditRecordFirstOriginal.objects)
        Assert.assertEquals(recordsLoaded[1].unixTimeStamp, auditRecordFirstOriginal.unixTimeStamp)

    }

}
