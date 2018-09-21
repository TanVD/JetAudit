package tanvd.jetaudit.auditDao.loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.jetaudit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.jetaudit.model.external.equal
import tanvd.jetaudit.model.external.presenters.StringPresenter
import tanvd.jetaudit.model.external.types.information.InformationType
import tanvd.jetaudit.utils.StringInf
import tanvd.jetaudit.utils.TestUtil

internal class NonValidQueryTest {


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
    fun tryStringSqlInjectionWithQuoteToAuditType_Equal() {
        val stringInjection = "'; Select * from example.Audit; --"

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection)
        Assert.assertEquals(elements.size, 0)
    }

    @Test
    fun tryStringSqlInjectionWithBackQuoteToAuditType_Equal() {
        val stringInjection = "`; Select * from example.Audit; --"

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection)
        Assert.assertEquals(elements.size, 0)
    }

    @Test
    fun tryStringWithEscapesToAuditType_Equal() {
        val stringInjection = "'`\n\b\t\\--"

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection)
        Assert.assertEquals(elements.size, 0)
    }


}
