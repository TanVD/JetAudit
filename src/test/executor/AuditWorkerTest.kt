package executor

import org.mockito.Mockito
import org.mockito.Mockito.*
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.AuditExecutor
import tanvd.audit.implementation.AuditWorker
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.implementation.writer.AuditReserveWriter
import tanvd.audit.model.internal.AuditRecordInternal
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(fullyQualifiedNames = arrayOf("tanvd.audit.implementation.*"))
internal class AuditWorkerTest : PowerMockTestCase() {

    private var auditDao: AuditDao? = null

    private var auditWriter: AuditReserveWriter? = null

    private var buffer: MutableList<AuditRecordInternal> = ArrayList()

    private var reserveBuffer: MutableList<AuditRecordInternal> = ArrayList()

    private val queue = ArrayBlockingQueue<AuditRecordInternal>(1)

    @BeforeMethod
    fun setMocks() {
        auditDao = mock(AuditDao::class.java)
        auditWriter = mock(AuditReserveWriter::class.java)

    }

    @AfterMethod
    fun resetMocks() {
        buffer.clear()
        reserveBuffer.clear()
    }

    @Test
    fun saveBuffer_gotRecordNoException_passedRecordToDao() {
        val record = simpleAuditRecordInternal()
        val batchSize = 1
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)

        auditWorker.saveBuffer(batchSize)

        verify(auditDao)!!.saveRecords(listOf(record))
    }

    @Test
    fun saveBuffer_gotRecordNoException_recordDeleted() {
        val record = simpleAuditRecordInternal()
        val batchSize = 1
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)

        auditWorker.saveBuffer(batchSize)

        Assert.assertTrue(buffer.isEmpty())
    }

    @Test
    fun saveBuffer_gotRecordException_passedRecordToReserveBuffer() {
        val record = simpleAuditRecordInternal()
        val batchSize = 1
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        `when`(auditDao!!.saveRecords(listOf(record))).thenThrow(BasicDbException())

        auditWorker.saveBuffer(batchSize)

        Assert.assertEquals(auditWorker.reserveBuffer, listOf(record))
    }

    fun simpleAuditRecordInternal() : AuditRecordInternal {
        return AuditRecordInternal(emptyList(), 1)
    }
}
