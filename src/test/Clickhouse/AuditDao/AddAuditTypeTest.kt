package Clickhouse.AuditDao

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.DbType
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.AuditType
import tanvd.audit.model.external.types.AuditType.TypesResolution.resolveType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
import utils.TestClassFirst
import utils.TypeUtils

internal class AddAuditTypeTest {


    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
        var currentId = 0L

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
    fun saveRecordBefore_addType_loadRecordsReturnSavedRecord() {
        val arrayObjects = arrayListOf(Pair(resolveType(String::class), "string"))
        val auditRecordOriginal = AuditRecordInternal(arrayObjects, getSampleInformation())
        auditDao!!.saveRecord(auditRecordOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNotNew() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSamplePrimitiveArrayObjects(), getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val auditRecordSecondOriginal = AuditRecordInternal(getSampleNonPrimitiveArrayObjects(), getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)

        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", QueryParameters())

        Assert.assertEquals(recordsLoaded, listOf(auditRecordFirstOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedNew() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSamplePrimitiveArrayObjects(), getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val auditRecordSecondOriginal = AuditRecordInternal(getSampleNonPrimitiveArrayObjects(), getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)


        val recordsLoaded = auditDao!!.loadRecords(TestClassFirst::class equal TestClassFirst(), QueryParameters())
        Assert.assertEquals(recordsLoaded, listOf(auditRecordSecondOriginal))
    }

    @Test
    fun saveRecordBefore_addTypeAndSaveWithNewType_loadRecordsLoadedBoth() {
        val auditRecordFirstOriginal = AuditRecordInternal(getSamplePrimitiveArrayObjects(), getSampleInformation())
        auditDao!!.saveRecord(auditRecordFirstOriginal)

        @Suppress("UNCHECKED_CAST")
        val typeTestClassFirst = AuditType(TestClassFirst::class, "TestClassFirst", TestClassFirst) as AuditType<Any>
        AuditType.addType(typeTestClassFirst)
        auditDao!!.addTypeInDbModel(typeTestClassFirst)

        val arrayObjectsSecond = arrayListOf(
                Pair(resolveType(String::class), "string"),
                Pair(resolveType(TestClassFirst::class), "TestClassFirstId"))
        val auditRecordSecondOriginal = AuditRecordInternal(arrayObjectsSecond, getSampleInformation())
        auditDao!!.saveRecord(auditRecordSecondOriginal)


        val recordsLoaded = auditDao!!.loadRecords(String::class equal "string", QueryParameters())
        Assert.assertEquals(recordsLoaded.toSet(), setOf(auditRecordFirstOriginal, auditRecordSecondOriginal))
    }

    private fun getSamplePrimitiveArrayObjects(postFixFirst: String = "", postFixSecond: String = ""):
            List<Pair<AuditType<Any>, String>> {
        return arrayListOf(Pair(resolveType(String::class), "string" + postFixFirst),
                Pair(resolveType(Int::class), "123" + postFixSecond))
    }

    private fun getSampleNonPrimitiveArrayObjects(): MutableList<Pair<AuditType<Any>, String>> {
        return arrayListOf(
                Pair(resolveType(utils.TestClassFirst::class), "TestClassFirstId"))
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(currentId++, 1, 2)
    }
}
