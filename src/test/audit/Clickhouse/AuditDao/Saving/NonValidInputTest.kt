package audit.Clickhouse.AuditDao.Saving

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import audit.utils.*
import audit.utils.SamplesGenerator.getRecordInternal

internal class NonValidInputTest {


    companion object {
        var auditDao: AuditDaoClickhouseImpl? = null
    }

    @BeforeMethod
    @Suppress("UNCHECKED_CAST")
    fun createAll() {
        auditDao = TestUtil.create()

        @Suppress("UNCHECKED_CAST")
        InformationType.addType(StringInf)
        auditDao!!.addInformationInDbModel(StringInf)
    }

    @AfterMethod
    fun clearAll() {
        TestUtil.drop()
    }

    @Test
    fun tryStringSqlInjectionWithQuoteToAuditType() {
        val stringInjection = "'; Select * from example.Audit; --"

        val information = getSampleInformation()
        information.add(InformationObject("", InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(stringInjection, information = information)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection)
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithBackQuoteToAuditType() {
        val stringInjection = "`; Select * from example.Audit; --"

        val information = getSampleInformation()
        information.add(InformationObject("", InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(stringInjection, information = information)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection)
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringWithEscapesToAuditType() {
        val stringInjection = "'`\n\b\t\\--"

        val information = getSampleInformation()
        information.add(InformationObject("", InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(stringInjection, information = information)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection)
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithQuoteToInformationType() {
        val stringInjection = "'; Select * from example.Audit; --"
        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringInf equal stringInjection)
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringSqlInjectionWithBackQuoteToInformationType() {
        val stringInjection = "`; Select * from example.Audit; --"

        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringInf equal stringInjection)
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    @Test
    fun tryStringWithEscapesToInformationType() {
        val stringInjection = "'`\n\b\t\\--"

        val informationObject = getSampleInformation()
        informationObject.add(InformationObject(stringInjection, InformationType.resolveType("StringInfColumn")))
        val auditRecordOriginal = getRecordInternal(information = informationObject)

        auditDao!!.saveRecord(auditRecordOriginal)

        val elements = auditDao!!.loadRecords(StringInf equal stringInjection)
        Assert.assertEquals(elements, listOf(auditRecordOriginal))
    }

    private fun getSampleInformation(): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(SavingTest.currentId++, 1, 2, SamplesGenerator.getMillenniumStart())
    }

}
