package Clickhouse.AuditDao.Saving

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
import tanvd.audit.model.external.types.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
import utils.StringPresenter
import utils.TypeUtils

internal class NonValidInputTest {


    companion object {
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
    }

    @Test
    fun tryStringSqlInjectionWithQuoteToAuditType() {
        val stringInjection = "'; Select * from example.Audit; --"

        val arrayObjects = arrayListOf(Pair(AuditType.resolveType(String::class), stringInjection))
        val auditRecordOriginal = AuditRecordInternal(arrayObjects, getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(String::class equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithBackQuoteToAuditType() {
        val stringInjection = "`; Select * from example.Audit; --"

        val arrayObjects = arrayListOf(Pair(AuditType.resolveType(String::class), stringInjection))
        val auditRecordOriginal = AuditRecordInternal(arrayObjects, getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(String::class equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringWithEscapesToAuditType() {
        val stringInjection = "'`\n\b\t\\--"

        val arrayObjects = arrayListOf(Pair(AuditType.resolveType(String::class), stringInjection))
        val auditRecordOriginal = AuditRecordInternal(arrayObjects, getSampleInformation())

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(String::class equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithQuoteToInformationType() {
        val stringInjection = "'; Select * from example.Audit; --"

        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringPresenter, "OneStringField", InformationType.InformationInnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("OneStringField")))
        val auditRecordOriginal = AuditRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithBackQuoteToInformationType() {
        val stringInjection = "`; Select * from example.Audit; --"

        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringPresenter, "OneStringField", InformationType.InformationInnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("OneStringField")))
        val auditRecordOriginal = AuditRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringWithEscapesToInformationType() {
        val stringInjection = "'`\n\b\t\\--"

        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringPresenter, "OneStringField", InformationType.InformationInnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("OneStringField")))
        val auditRecordOriginal = AuditRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    private fun getSampleInformation(): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(SavingTest.currentId++, 1, 2)
    }

}
