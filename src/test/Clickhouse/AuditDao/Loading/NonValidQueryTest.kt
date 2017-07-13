package Clickhouse.AuditDao.Loading

import Clickhouse.AuditDao.Loading.Information.InformationBoolean
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.external.presenters.StringPresenter
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.queries.equal
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationType
import utils.DbUtils
import utils.StringInfPresenter
import utils.TypeUtils

internal class NonValidQueryTest {


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
    fun tryStringSqlInjectionWithQuoteToAuditType_Equal() {
        val stringInjection = "'; Select * from example.Audit; --"

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection, QueryParameters())
        Assert.assertEquals(elements.size, 0)
    }

    @Test
    fun tryStringSqlInjectionWithBackQuoteToAuditType_Equal() {
        val stringInjection = "`; Select * from example.Audit; --"

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection, QueryParameters())
        Assert.assertEquals(elements.size, 0)
    }

    @Test
    fun tryStringWithEscapesToAuditType_Equal() {
        val stringInjection = "'`\n\b\t\\--"

        val elements = auditDao!!.loadRecords(StringPresenter.value equal stringInjection, QueryParameters())
        Assert.assertEquals(elements.size, 0)
    }


}
