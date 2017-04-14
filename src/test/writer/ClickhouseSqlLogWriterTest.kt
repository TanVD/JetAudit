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
import tanvd.audit.implementation.writer.ClickhouseSqlLogWriter
import tanvd.audit.model.external.AuditType
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.serializers.IntSerializer
import tanvd.audit.serializers.LongSerializer
import tanvd.audit.serializers.StringSerializer

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
internal class ClickhouseSqlLogWriterTest : PowerMockTestCase() {

    @BeforeMethod
    fun init() {
        AuditType.addType(AuditType(String::class, "Type_String", StringSerializer) as AuditType<Any>)
        AuditType.addType(AuditType(Int::class, "Type_Int", IntSerializer) as AuditType<Any>)
        AuditType.addType(AuditType(Long::class, "Type_Long", LongSerializer) as AuditType<Any>)
    }

    @AfterMethod
    fun clean() {
        AuditType.clearTypes()
    }

    @Test
    fun write_gotAuditRecordInternal_AppropriateSqlInsertWritten() {
        val auditRecord = AuditRecordInternal(123, 456L, unixTimeStamp = 1)
        val logWriter = PowerMockito.mock(Logger::class.java)
        val reserveWriter = ClickhouseSqlLogWriter(logWriter)

        reserveWriter.write(auditRecord)

        val typeInt = AuditType.resolveType(Int::class)
        val typeLong = AuditType.resolveType(Long::class)

        Mockito.verify(logWriter).error("INSERT INTO ${AuditDaoClickhouseImpl.auditTable} (" +
                "${typeInt.code}, ${typeLong.code}, ${AuditDaoClickhouseImpl.descriptionColumn}," +
                " ${AuditDaoClickhouseImpl.unixTimeStampColumn}) VALUES " +
                "(['${typeInt.serialize(123)}'], ['${typeLong.serialize(456)}'], ['${typeInt.code}', '${typeLong.code}']," +
                " 1);")
    }
}
