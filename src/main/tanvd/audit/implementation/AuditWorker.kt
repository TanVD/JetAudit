package tanvd.audit.implementation

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.implementation.writer.AuditReserveWriter
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.PropertyLoader
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Saves audits in DB.
 *
 * Implements strategy of audits recovering.
 * In case of Db failure worker will be trying to save failed records until succeed.
 */
internal class AuditWorker {

    constructor(auditQueueInternal: BlockingQueue<AuditRecordInternal>) {
        this.auditQueueInternal = auditQueueInternal
        buffer = ArrayList()
        reserveBuffer = ArrayList()
        this.auditDao = AuditDao.getDao()
        this.auditReserveWriter = AuditReserveWriter.getWriter()
    }

    constructor(auditQueueInternal: BlockingQueue<AuditRecordInternal>, buffer: MutableList<AuditRecordInternal>,
                reserveBuffer: MutableList<AuditRecordInternal>,
                auditDao: AuditDao, auditReserveWriter: AuditReserveWriter) {
        this.auditQueueInternal = auditQueueInternal
        this.buffer = buffer
        this.reserveBuffer = reserveBuffer
        this.auditReserveWriter = auditReserveWriter
        this.auditDao = auditDao

    }

    companion object Config {
        val capacityOfWorkerBuffer = PropertyLoader.loadProperty("CapacityOfWorkerBuffer")?.toInt() ?: 5000
        val waitingQueueTime = PropertyLoader.loadProperty("WaitingQueueTime")?.toLong() ?: 10
        val maxGeneration = PropertyLoader.loadProperty("MaxGeneration")?.toInt() ?: 15
    }

    val auditQueueInternal: BlockingQueue<AuditRecordInternal>

    val buffer: MutableList<AuditRecordInternal>

    val reserveBuffer: MutableList<AuditRecordInternal>

    val auditDao: AuditDao

    val auditReserveWriter: AuditReserveWriter

    var isWorking: Boolean = true

    var isEnabled = true

    fun start() {
        while (true) {
            //while performing cycle worker can not report it's state to executor
            synchronized(isWorking) {
                //get new record if sure that can save it (reserve buffer not full)
                if (reserveBuffer.size != capacityOfWorkerBuffer) {
                    processNewRecord()
                }

                if (!reserveBuffer.isEmpty()) {
                    isWorking = true
                    processReserveBuffer()
                }
            }

            //stop if asked
            if (!isEnabled) {
                auditReserveWriter.close()
                break
            }
        }
    }

    fun processNewRecord() {
        //get batch size that surely can be saved to reserveBuffer
        val batchSize = capacityOfWorkerBuffer - reserveBuffer.size

        val record = auditQueueInternal.poll(waitingQueueTime, TimeUnit.MILLISECONDS)
        isWorking = record != null || buffer.isNotEmpty()
        if (record == null) {
            saveBuffer(batchSize)
        } else {
            if (buffer.size == capacityOfWorkerBuffer) {
                saveBuffer(batchSize)
            }
            buffer.add(record)
        }
    }


    /**
     * Saves AuditWorker buffer to Db.
     * Add records to reserve queue if exceptional situation occurred
     */
    fun saveBuffer(batchSize: Int) {
        if (buffer.isEmpty()) {
            return
        }
        val records = buffer.take(batchSize)
        try {
            auditDao.saveRecords(records)
        } catch (e: BasicDbException) {
            reserveBuffer.addAll(records)
        }
        for (i in 1..batchSize) {
            if (buffer.isEmpty()) {
                break
            }
            buffer.removeAt(0)
        }
    }

    /**
     * Save AuditWorker reserve buffer to Db one by one record.
     * If record was not saved then generation of record will be incremented.
     * If generation equal to max generation than record will be printed by writer
     */
    fun processReserveBuffer() {
        val iterator = reserveBuffer.iterator()
        while (iterator.hasNext()) {
            val record = iterator.next()
            var successSaved = true

            try {
                auditDao.saveRecord(record)
            } catch (e: BasicDbException) {
                record.generation++
                successSaved = false
            }

            if (!successSaved && record.generation == maxGeneration) {
                auditReserveWriter.write(record)
                iterator.remove()
            } else if (successSaved) {
                iterator.remove()
            }
        }

        auditReserveWriter.flush()
    }
}

