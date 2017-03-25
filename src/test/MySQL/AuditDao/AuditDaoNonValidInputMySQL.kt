package MySQL.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.implementation.mysql.AuditDaoMysqlImpl
import tanvd.audit.model.AuditRecord
import tanvd.audit.model.AuditType
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.StringSerializer


internal class AuditDaoNonValidInputMySQL {

    companion object {
        var auditDao : AuditDaoMysqlImpl? = null
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
    fun tryStringSqlInjectionWithQuote() {
        val stringInjection = "'; Select * from example.Audit; --"

        val arrayObjects = arrayListOf(
                Pair(AuditType.resolveType(String::class), stringInjection))
        val auditRecordOriginal = AuditRecord(arrayObjects, 127)
        auditDao!!.saveRecord(auditRecordOriginal)
        val elements = auditDao!!.loadRecords(AuditType.resolveType(String::class), stringInjection)
        Assert.assertEquals(elements.size, 1)
        Assert.assertEquals(elements[0].objects[0].second, stringInjection)
    }

    @Test
    fun tryStringSqlInjectionWithBackQuote() {
        val stringInjection = "`; Select * from example.Audit; --"

        val arrayObjects = arrayListOf(
                Pair(AuditType.resolveType(String::class), stringInjection))
        val auditRecordOriginal = AuditRecord(arrayObjects, 127)
        auditDao!!.saveRecord(auditRecordOriginal)
        val elements = auditDao!!.loadRecords(AuditType.resolveType(String::class), stringInjection)
        Assert.assertEquals(elements.size, 1)
        Assert.assertEquals(elements[0].objects[0].second, stringInjection)
    }

    @Test
    fun tryStringWithEscapes() {
        val stringInjection = "'`\n\b\t\\--"

        val arrayObjects = arrayListOf(
                Pair(AuditType.resolveType(String::class), stringInjection))
        val auditRecordOriginal = AuditRecord(arrayObjects, 127)
        auditDao!!.saveRecord(auditRecordOriginal)
        val elements = auditDao!!.loadRecords(AuditType.resolveType(String::class), stringInjection)
        Assert.assertEquals(elements.size, 1)
        Assert.assertEquals(elements[0].objects[0].second, stringInjection)
    }

}