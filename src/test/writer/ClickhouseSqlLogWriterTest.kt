package writer

import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.modules.testng.PowerMockTestCase
import org.slf4j.Logger
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.clickhouse.model.getCode
import tanvd.audit.implementation.writer.ClickhouseSqlLogWriter
import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.InformationUtils
import utils.SamplesGenerator.getRecordInternal
import utils.TypeUtils

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
internal class ClickhouseSqlLogWriterTest : PowerMockTestCase() {

    @BeforeMethod
    fun init() {
        TypeUtils.addAuditTypesPrimitive()
        TypeUtils.addInformationTypesPrimitive()
    }

    @AfterMethod
    fun clean() {
        TypeUtils.clearTypes()
    }

    @Test
    fun write_gotAuditRecordInternal_AppropriateSqlInsertWritten() {
        val id = 0L
        val version = 1L
        val timeStamp = 2L
        val auditRecord = getRecordInternal(123, 456L, information = getSampleInformation(id, timeStamp, version))
        val logWriter = PowerMockito.mock(Logger::class.java)
        val reserveWriter = ClickhouseSqlLogWriter(logWriter)

        reserveWriter.write(auditRecord)


        Mockito.verify(logWriter).error("INSERT INTO ${AuditDaoClickhouseImpl.auditTable} (" +
                "${IntPresenter.value.getCode()}, ${LongPresenter.value.getCode()}, ${AuditDaoClickhouseImpl.descriptionColumn}, " +
                "${InformationType.resolveType(IdPresenter).code}, " +
                "${InformationType.resolveType(VersionPresenter).code}, " +
                "${InformationType.resolveType(TimeStampPresenter).code}) VALUES " +
                "([123], [456], [Int, Long], 0, 1, 2);")
    }

    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long): MutableSet<InformationObject> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, version)
    }
}
