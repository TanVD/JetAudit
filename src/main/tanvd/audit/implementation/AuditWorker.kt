package tanvd.audit.implementation

import org.jetbrains.annotations.TestOnly
import org.slf4j.LoggerFactory
import tanvd.audit.implementation.clickhouse.AuditDao
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouse
import tanvd.audit.implementation.writer.AuditReserveWriter
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.Conf
import tanvd.audit.utils.PropertyLoader
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Saves audits in DB.
 *
 * In case of Db failure worker will be trying to save failed for some time and then save will save
 * records to failover.
 */
internal class AuditWorker {

    constructor(auditQueueInternal: BlockingQueue<QueueCommand>) {
        this.auditQueueInternal = auditQueueInternal
        buffer = ArrayList()
        reserveBuffer = ArrayList()
        this.auditDao = AuditDaoClickhouse()
        this.auditReserveWriter = AuditReserveWriter.getWriter()
    }

    @TestOnly
    constructor(auditQueueInternal: BlockingQueue<QueueCommand>, buffer: MutableList<AuditRecordInternal>,
                reserveBuffer: MutableList<AuditRecordInternal>,
                auditDao: AuditDao, auditReserveWriter: AuditReserveWriter) {
        this.auditQueueInternal = auditQueueInternal
        this.buffer = buffer
        this.reserveBuffer = reserveBuffer
        this.auditReserveWriter = auditReserveWriter
        this.auditDao = auditDao
    }

    companion object Config {
        val capacityOfWorkerBuffer by lazy { PropertyLoader[Conf.WORKER_BUFFER].toInt() }
        val waitingQueueTime by lazy { PropertyLoader[Conf.WAITING_QUEUE_TIME].toLong() }
        val maxGeneration by lazy { PropertyLoader[Conf.MAX_GENERATION].toInt() }
    }

    private val auditQueueInternal: BlockingQueue<QueueCommand>

    internal val buffer: MutableList<AuditRecordInternal>

    internal val reserveBuffer: MutableList<AuditRecordInternal>

    private val auditDao: AuditDao

    private val auditReserveWriter: AuditReserveWriter

    private val logger = LoggerFactory.getLogger(AuditWorker::class.java)

    private var isEnabled = true

    fun start() {
        while (!Thread.interrupted()) {
            try {
                //while performing cycle worker can not report it's state to executor
                //get new record if sure that can save it (reserve buffer not full)
                if (reserveBuffer.size != capacityOfWorkerBuffer) {
                    processNewRecord()
                }

                if (!reserveBuffer.isEmpty()) {
                    processReserveBuffer()
                }


                //stop if asked
                if (!isEnabled) {
                    auditReserveWriter.close()
                    return
                }
            } catch (e: InterruptedException) {
                logger.info("Audit worker exiting.")
                return
            } catch (e: Throwable) {
                logger.error("Audit worker encountered error", e)
            }
        }
    }

    fun processNewRecord() {
        //get batch size that surely can be saved to reserveBuffer
        val batchSize = capacityOfWorkerBuffer - reserveBuffer.size

        val record = auditQueueInternal.poll(waitingQueueTime, TimeUnit.MILLISECONDS)
        when (record) {
            null -> {
                saveBuffer(batchSize)
            }
            is SaveRecords -> {
                if (buffer.size == capacityOfWorkerBuffer) {
                    saveBuffer(batchSize)
                }
                buffer += record.recordsInternal
            }
            is ShutDown -> {
                saveBuffer(batchSize)
                isEnabled = false
            }
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
        } catch (e: InterruptedException) {
            isEnabled = false
            return
        } catch (e: Exception) {
            logger.error("Encountered error while saving records", e)
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
     *
     * If Reserve Buffer is full all records will be printed by writer
     */
    fun processReserveBuffer() {
        val iterator = reserveBuffer.iterator()
        val reserveBufferFull = reserveBuffer.size == capacityOfWorkerBuffer
        if (reserveBufferFull) {
            logger.error("Reserve buffer full. Flushing to reserve writer.")
        }
        while (iterator.hasNext()) {
            val record = iterator.next()
            if (reserveBufferFull) {
                auditReserveWriter.write(record)
                iterator.remove()
            } else {
                var successSaved = true

                try {
                    auditDao.saveRecord(record)
                } catch (e: InterruptedException) {
                    isEnabled = false
                    return
                } catch (e: Exception) {
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

        }

        auditReserveWriter.flush()
    }

    @TestOnly
    fun disable() {
        isEnabled = false
    }
}

