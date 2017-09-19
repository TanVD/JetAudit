package writer

import com.amazonaws.services.s3.AmazonS3
import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.clickhouse.model.getCode
import tanvd.audit.implementation.writer.ClickhouseSqlS3Writer
import tanvd.audit.model.external.presenters.*
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import utils.InformationUtils
import utils.SamplesGenerator
import utils.TypeUtils

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
internal class ClickhouseSqlS3WriterTest : PowerMockTestCase() {

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
        val auditRecord = SamplesGenerator.getRecordInternal(123, "456", information = getSampleInformation(id, timeStamp, version))
        val s3Client = PowerMockito.mock(AmazonS3::class.java)
        val reserveWriter = ClickhouseSqlS3Writer(s3Client)

        reserveWriter.write(auditRecord)
        reserveWriter.flush()


        Mockito.verify(s3Client).putObject(Mockito.eq("ClickhouseFailover"), Mockito.anyString(),
                Mockito.eq("INSERT INTO ${AuditDaoClickhouseImpl.auditTable} (" +
                        "${IntPresenter.value.getCode()}, ${StringPresenter.value.getCode()}, ${AuditDaoClickhouseImpl.descriptionColumn}, " +
                        "${InformationType.resolveType(IdPresenter).code}, " +
                        "${InformationType.resolveType(VersionPresenter).code}, " +
                        "${InformationType.resolveType(TimeStampPresenter).code}, " +
                        "${InformationType.resolveType(DatePresenter).code}, " +
                        "${InformationType.resolveType(IsDeletedPresenter).code}) VALUES " +
                        "([123], ['456'], ['Int', 'String'], 0, 1, 2, '2000-01-01', 0);"))
    }

    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long): MutableSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, version, SamplesGenerator.getMillenniumStart())
    }
}