package executor

import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.audit.implementation.AuditWorker
import tanvd.audit.implementation.QueueCommand
import tanvd.audit.implementation.SaveRecords
import tanvd.audit.implementation.ShutDown
import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.implementation.writer.AuditReserveWriter
import tanvd.audit.model.external.records.InformationObject
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.internal.AuditRecordInternal
import utils.StringInf
import java.util.concurrent.ArrayBlockingQueue

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(fullyQualifiedNames = arrayOf("tanvd.audit.implementation.*"))
internal class WorkerTest : PowerMockTestCase() {

    private var auditDao: AuditDao? = null

    private var auditWriter: AuditReserveWriter? = null

    private var buffer: MutableList<AuditRecordInternal> = ArrayList()

    private var reserveBuffer: MutableList<AuditRecordInternal> = ArrayList()

    private val queue = ArrayBlockingQueue<QueueCommand>(1)

    @BeforeMethod
    fun setMocks() {
        auditDao = PowerMockito.mock(AuditDao::class.java)
        auditWriter = PowerMockito.mock(AuditReserveWriter::class.java)

    }

    @AfterMethod
    fun resetMocks() {
        buffer.clear()
        reserveBuffer.clear()
    }


    @Test
    fun start_bufferNotFullReserveBufferEmpty_CalledProcessNewRecord() {
        val record = simpleAuditRecordInternalFirst()
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        queue.add(ShutDown())
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.start()

        Mockito.verify(spyWorker).processNewRecord()
    }

    @Test
    fun start_bufferNotFullReserveBufferEmpty_NotCalledProcessReserveBuffer() {
        val record = simpleAuditRecordInternalFirst()
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        queue.add(ShutDown())
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.start()

        Mockito.verify(spyWorker, Mockito.never()).processReserveBuffer()
    }

    @Test
    fun start_bufferFullReserveBufferEmpty_NotCalledProcessNewRecord() {
        val record = simpleAuditRecordInternalFirst()
        for (i in 1..AuditWorker.capacityOfWorkerBuffer) {
            reserveBuffer.add(record)
        }
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        auditWorker.disable()
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.start()

        Mockito.verify(spyWorker, Mockito.never()).processNewRecord()

    }

    @Test
    fun start_bufferFullReserveBufferNotEmpty_CalledProcessReserveBuffer() {
        val record = simpleAuditRecordInternalFirst()
        reserveBuffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        queue.add(ShutDown())
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.start()

        Mockito.verify(spyWorker).processReserveBuffer()
    }

    //Testing contracts of parts

    //save buffer

    @Test
    fun saveBuffer_gotRecordNoException_passedRecordToDao() {
        val record = simpleAuditRecordInternalFirst()
        val batchSize = 1
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)

        auditWorker.saveBuffer(batchSize)

        Mockito.verify(auditDao)!!.saveRecords(listOf(record))
    }

    @Test
    fun saveBuffer_gotRecordNoException_recordDeleted() {
        val record = simpleAuditRecordInternalFirst()
        val batchSize = 1
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)

        auditWorker.saveBuffer(batchSize)

        Assert.assertTrue(buffer.isEmpty())
    }

    @Test
    fun saveBuffer_gotRecordException_passedRecordToReserveBuffer() {
        val record = simpleAuditRecordInternalFirst()
        val batchSize = 1
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        PowerMockito.`when`(auditDao!!.saveRecords(listOf(record))).thenThrow(BasicDbException())

        auditWorker.saveBuffer(batchSize)

        Assert.assertEquals(auditWorker.reserveBuffer, listOf(record))
    }

    //process new record

    @Test
    fun processNewRecord_recordNullReserveBufferEmpty_calledSaveBufferWithCapacityOfWorkerBuffer() {
        val record = simpleAuditRecordInternalFirst()
        buffer.add(record)
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processNewRecord()

        Mockito.verify(spyWorker).saveBuffer(AuditWorker.capacityOfWorkerBuffer)
    }

    @Test
    fun processNewRecord_recordNullReserveBufferFull_calledSaveBufferWithZero() {
        val record = simpleAuditRecordInternalFirst()
        buffer.add(record)
        for (i in 1..AuditWorker.capacityOfWorkerBuffer) {
            reserveBuffer.add(record)
        }
        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processNewRecord()

        Mockito.verify(spyWorker).saveBuffer(0)
    }

    @Test
    fun processNewRecord_recordNotNullBufferNotFull_addRecordToBuffer() {
        val recordFirst = simpleAuditRecordInternalFirst()
        buffer.add(recordFirst)
        val recordSecond = simpleAuditRecordInternalSecond()
        queue.add(SaveRecords(recordSecond))

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processNewRecord()

        Assert.assertEquals(spyWorker.buffer.size, 2)
        Assert.assertEquals(spyWorker.buffer.toSet(), setOf(recordFirst, recordSecond))
    }

    @Test
    fun processNewRecord_recordNotNullBufferFullReserveBufferEmpty_calledSaveBufferWithCapacityOfWorkerBuffer() {
        val recordFirst = simpleAuditRecordInternalFirst()
        for (i in 1..AuditWorker.capacityOfWorkerBuffer) {
            buffer.add(recordFirst)
        }
        val recordSecond = simpleAuditRecordInternalSecond()
        queue.add(SaveRecords(recordSecond))

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processNewRecord()

        Mockito.verify(spyWorker).saveBuffer(AuditWorker.capacityOfWorkerBuffer)
    }

    @Test
    fun processNewRecord_recordNotNullBufferFullReserveBufferEmpty_addRecordToEmptyNowBuffer() {
        val recordFirst = simpleAuditRecordInternalFirst()
        for (i in 1..AuditWorker.capacityOfWorkerBuffer) {
            buffer.add(recordFirst)
        }
        val recordSecond = simpleAuditRecordInternalSecond()
        queue.add(SaveRecords(recordSecond))

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processNewRecord()

        Assert.assertEquals(spyWorker.buffer.size, 1)
        Assert.assertEquals(spyWorker.buffer.toSet(), setOf(recordSecond))
    }

    @Test
    fun processNewRecord_recordNotNullBufferFullReserveBufferFiveElements_calledSaveBufferWithCapacityOfWorkerBufferMinusFive() {
        val recordFirst = simpleAuditRecordInternalFirst()
        for (i in 1..AuditWorker.capacityOfWorkerBuffer) {
            buffer.add(recordFirst)
        }
        for (i in 1..5) {
            reserveBuffer.add(recordFirst)
        }
        val recordSecond = simpleAuditRecordInternalSecond()
        queue.add(SaveRecords(recordSecond))

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processNewRecord()

        Mockito.verify(spyWorker).saveBuffer(AuditWorker.capacityOfWorkerBuffer - 5)
    }

    @Test
    fun processNewRecord_recordNotNullBufferFullReserveBufferFiveElements_addRecordToFiveElementsNowBuffer() {
        val recordFirst = simpleAuditRecordInternalFirst()
        for (i in 1..AuditWorker.capacityOfWorkerBuffer) {
            buffer.add(recordFirst)
        }
        for (i in 1..5) {
            reserveBuffer.add(recordFirst)
        }
        val recordSecond = simpleAuditRecordInternalSecond()
        queue.add(SaveRecords(recordSecond))

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processNewRecord()

        Assert.assertEquals(spyWorker.buffer.size, 6)
        Assert.assertEquals(spyWorker.buffer.last(), recordSecond)
        for (i in 0..4) {
            Assert.assertEquals(spyWorker.buffer[i], recordFirst)
        }
    }

    //process reserve buffer

    @Test
    fun processReserveBuffer_noExceptionalRecords_allRecordsPassedToDaoOneByOne() {
        val recordFirst = simpleAuditRecordInternalFirst()
        val recordSecond = simpleAuditRecordInternalSecond()
        reserveBuffer.add(recordFirst)
        reserveBuffer.add(recordSecond)

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processReserveBuffer()

        Mockito.verify(auditDao)!!.saveRecord(recordFirst)
        Mockito.verify(auditDao)!!.saveRecord(recordSecond)
    }

    @Test
    fun processReserveBuffer_noExceptionalRecords_allRecordsDeletedFromReserveBuffer() {
        val recordFirst = simpleAuditRecordInternalFirst()
        val recordSecond = simpleAuditRecordInternalSecond()
        reserveBuffer.add(recordFirst)
        reserveBuffer.add(recordSecond)

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processReserveBuffer()

        Assert.assertTrue(auditWorker.reserveBuffer.isEmpty())
    }

    @Test
    fun processReserveBuffer_oneRecordExceptionalNotLastGeneration_NonExceptionalRecordDeletedFromReserveBuffer() {
        val recordFirst = simpleAuditRecordInternalFirst()
        val recordSecond = simpleAuditRecordInternalSecond()
        reserveBuffer.add(recordFirst)
        reserveBuffer.add(recordSecond)
        PowerMockito.`when`(auditDao!!.saveRecord(recordFirst)).thenThrow(BasicDbException::class.java)

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processReserveBuffer()

        Assert.assertEquals(auditWorker.reserveBuffer.size, 1)
        Assert.assertEquals(auditWorker.reserveBuffer.toSet(), setOf(recordFirst))
    }

    @Test
    fun processReserveBuffer_oneRecordExceptionalNotLastGeneration_ExceptionalRecordGotNewGeneration() {
        val recordFirst = simpleAuditRecordInternalFirst()
        val recordSecond = simpleAuditRecordInternalSecond()
        reserveBuffer.add(recordFirst)
        reserveBuffer.add(recordSecond)
        PowerMockito.`when`(auditDao!!.saveRecord(recordFirst)).thenThrow(BasicDbException::class.java)

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processReserveBuffer()

        Assert.assertEquals(recordFirst.generation, 2)
    }

    @Test
    fun processReserveBuffer_oneRecordExceptionalLastGeneration_AllRecordsDeletedFromReserveBuffer() {
        val recordFirst = simpleAuditRecordInternalFirst()
        recordFirst.generation = AuditWorker.maxGeneration - 1
        val recordSecond = simpleAuditRecordInternalSecond()
        reserveBuffer.add(recordFirst)
        reserveBuffer.add(recordSecond)
        PowerMockito.`when`(auditDao!!.saveRecord(recordFirst)).thenThrow(BasicDbException::class.java)

        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
        val spyWorker = PowerMockito.spy(auditWorker)

        spyWorker.processReserveBuffer()

        Assert.assertTrue(auditWorker.reserveBuffer.isEmpty())
    }

//    @Test
//    fun processReserveBuffer_oneRecordExceptionalLastGeneration_ExceptionalRecordPassedToWriter() {
//        val recordFirst = simpleAuditRecordInternalFirst()
//        recordFirst.generation = AuditWorker.maxGeneration - 1
//        val recordSecond = simpleAuditRecordInternalSecond()
//        reserveBuffer.add(recordFirst)
//        reserveBuffer.add(recordSecond)
//        PowerMockito.`when`(auditDao!!.saveRecord(recordFirst)).thenThrow(BasicDbException::class.java)
//
//        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
//        val spyWorker = PowerMockito.spy(auditWorker)
//
//        spyWorker.processReserveBuffer()
//
//        Mockito.verify(auditWriter)!!.write(recordFirst)
//    }

//    @Test
//    fun processReserveBuffer_oneRecordExceptionalLastGeneration_WriterFlushedAtTheEnd() {
//        val recordFirst = simpleAuditRecordInternalFirst()
//        recordFirst.generation = AuditWorker.maxGeneration - 1
//        val recordSecond = simpleAuditRecordInternalSecond()
//        reserveBuffer.add(recordFirst)
//        reserveBuffer.add(recordSecond)
//        PowerMockito.`when`(auditDao!!.saveRecord(recordFirst)).thenThrow(BasicDbException::class.java)
//
//        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
//        val spyWorker = PowerMockito.spy(auditWorker)
//
//        spyWorker.processReserveBuffer()
//
//        val order = Mockito.inOrder(auditWriter)
//
//        order.verify(auditWriter)!!.write(recordFirst)
//
//        order.verify(auditWriter)!!.flush()
//    }

//    @Test
//    fun processReserveBuffer_BothRecordExceptionalLastGeneration_WriterFlushedAtTheEndOneTime() {
//        val recordFirst = simpleAuditRecordInternalFirst()
//        recordFirst.generation = AuditWorker.maxGeneration - 1
//        val recordSecond = simpleAuditRecordInternalSecond()
//        recordSecond.generation = AuditWorker.maxGeneration - 1
//
//        reserveBuffer.add(recordFirst)
//        reserveBuffer.add(recordSecond)
//        PowerMockito.`when`(auditDao!!.saveRecord(recordFirst)).thenThrow(BasicDbException::class.java)
//        PowerMockito.`when`(auditDao!!.saveRecord(recordSecond)).thenThrow(BasicDbException::class.java)
//
//
//        val auditWorker = AuditWorker(queue, buffer, reserveBuffer, auditDao!!, auditWriter!!)
//        val spyWorker = PowerMockito.spy(auditWorker)
//
//        spyWorker.processReserveBuffer()
//
//        val order = Mockito.inOrder(auditWriter)
//
//        order.verify(auditWriter)!!.write(recordFirst)
//
//        order.verify(auditWriter)!!.write(recordSecond)
//
//        order.verify(auditWriter)!!.flush()
//    }

    fun simpleAuditRecordInternalFirst(): AuditRecordInternal {
        return AuditRecordInternal(emptyList(), getSampleInformation("string1"))
    }

    fun simpleAuditRecordInternalSecond(): AuditRecordInternal {
        return AuditRecordInternal(emptyList(), getSampleInformation("string2"))
    }

    private fun getSampleInformation(value: String): LinkedHashSet<InformationObject<*>> {
        @Suppress("UNCHECKED_CAST")
        return LinkedHashSet(setOf(InformationObject(value, StringInf)))
    }
}
