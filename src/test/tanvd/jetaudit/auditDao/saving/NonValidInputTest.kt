package tanvd.jetaudit.auditDao.saving

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.presenters.StringPresenter
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.utils.InformationUtils
import tanvd.jetaudit.utils.SamplesGenerator
import tanvd.jetaudit.utils.SamplesGenerator.getRecordInternal
import tanvd.jetaudit.utils.StringInf
import tanvd.jetaudit.utils.TestUtil

internal class NonValidInputTest {


    companion object {
        var auditDao: AuditDaoClickhouse? = null
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
