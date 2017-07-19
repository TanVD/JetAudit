package Clickhouse.AuditDao.Saving

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationType
import utils.*
import utils.SamplesGenerator.getRecordInternal

internal class NonValidInputTest {


    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {

        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()

        AuditDao.credentials = DbUtils.getCredentials()
        auditDao = AuditDao.getDao() as AuditDaoClickhouseImpl

        @Suppress("UNCHECKED_CAST")
        val type = InformationType(StringInfPresenter, InnerType.String) as InformationType<Any>
        InformationType.addType(type)
        auditDao!!.addInformationInDbModel(type)

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

        val information = getSampleInformation()
        information.add(InformationObject("", InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(stringInjection, information = information)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithBackQuoteToAuditType() {
        val stringInjection = "`; Select * from example.Audit; --"

        val information = getSampleInformation()
        information.add(InformationObject("", InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(stringInjection, information = information)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringWithEscapesToAuditType() {
        val stringInjection = "'`\n\b\t\\--"

        val information = getSampleInformation()
        information.add(InformationObject("", InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(stringInjection, information = information)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithQuoteToInformationType() {
        val stringInjection = "'; Select * from example.Audit; --"
        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringInfPresenter equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithBackQuoteToInformationType() {
        val stringInjection = "`; Select * from example.Audit; --"

        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringInfPresenter equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringWithEscapesToInformationType() {
        val stringInjection = "'`\n\b\t\\--"

        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringInfPresenter equal stringInjection, QueryParameters())
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    private fun getSampleInformation(): MutableSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(SavingTest.currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

}
