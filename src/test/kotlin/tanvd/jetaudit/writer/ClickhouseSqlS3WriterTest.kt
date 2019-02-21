package tanvd.jetaudit.writer

import com.amazonaws.services.s3.AmazonS3
import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.jetaudit.implementation.clickhouse.aorm.AuditTable
import tanvd.jetaudit.implementation.writer.ClickhouseSqlS3Writer
import tanvd.jetaudit.model.external.presenters.*
import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.utils.InformationUtils
import tanvd.jetaudit.utils.SamplesGenerator
import tanvd.jetaudit.utils.TestUtil

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
internal class ClickhouseSqlS3WriterTest : PowerMockTestCase() {

    @BeforeMethod
    fun init() {
        TestUtil.create()
    }

    @AfterMethod
    fun clean() {
        TestUtil.drop()
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
                Mockito.eq("INSERT INTO ${AuditTable.name} (" +
                        "${IntPresenter.value.column.name}, ${StringPresenter.value.column.name}, " +
                        "${IdType.column.name}, " +
                        "${TimeStampType.column.name}, " +
                        "${VersionType.column.name}, " +
                        "${DateType.column.name}, " +
                        "${IsDeletedType.column.name}, " +
                        "${AuditTable.description.name}, " +
                        "${LongPresenter.value.column.name}) VALUES " +
                        "([123], ['456'], 0, 2, 1, '2000-01-01', 0, ['Int', 'String'], []);"))
    }

    private fun getSampleInformation(id: Long, timeStamp: Long, version: Long): LinkedHashSet<InformationObject<*>> {
        return InformationUtils.getPrimitiveInformation(id, timeStamp, version, SamplesGenerator.getMillenniumStart())
    }
}