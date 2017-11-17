package clickhouse.auditDao.loading

import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.audit.model.external.equal
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.types.information.InformationType
import utils.StringInf
import utils.TestUtil

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
