package tanvd.audit.implementation

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.PropertyLoader
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Starts audit saving workers
 */
internal class AuditExecutor(val auditQueueInternal: BlockingQueue<AuditRecordInternal>) {

    companion object Config {
        val numberOfWorkers = PropertyLoader.load("auditApiConfig.properties", "NumberOfWorkers").toInt()
        val capacityOfWorkerBuffer = PropertyLoader.load("auditApiConfig.properties", "CapacityOfWorkerBuffer").toInt()
        val waitingQueueTime = PropertyLoader.load("auditApiConfig.properties", "WaitingQueueTime").toLong()
    }

    val workers: List<AuditWorker>

    val executorService: ExecutorService = Executors.newFixedThreadPool(numberOfWorkers)

    init {
        val workersList = ArrayList<AuditWorker>()
        for (i in 1..numberOfWorkers) {
            executorService.execute {
                val auditWorker = AuditWorker(auditQueueInternal, capacityOfWorkerBuffer, waitingQueueTime)
                workersList.add(auditWorker)
                auditWorker.start()
            }
        }
        workers = workersList
    }

    fun isStillWorking(): Boolean {
        return workers.any { it.isWorking }
    }

    /**
     * Tries to stop workers. If success -- returns true.
     */
    fun stopWorkers(timeToWait: Long): Boolean {
        Thread.sleep(timeToWait)
        if (!isStillWorking()) {
            for (worker in workers) {
                worker.isEnabled = false
            }
            return true
        }
        return false
    }

    /**
     * Saves audits in DB
     */
    class AuditWorker(val auditQueueInternal: BlockingQueue<AuditRecordInternal>, val maxBuffer: Int,
                      val timeToWait: Long) {

        val buffer: MutableList<AuditRecordInternal> = ArrayList()

        val auditDao: AuditDao = AuditDao.getDao()

        var isWorking: Boolean = true

        var isEnabled = true

        //TODO Check if reconnection too slow
        fun start() {
            while (true) {
                synchronized(isWorking) {
                    val record = auditQueueInternal.poll(timeToWait, TimeUnit.MILLISECONDS)
                    isWorking = record != null || buffer.isNotEmpty()
                    if (record == null && buffer.isNotEmpty()) {
                        auditDao.saveRecords(buffer.take(buffer.size))
                        buffer.clear()
                    }
                    if (record != null) {
                        if (buffer.size == maxBuffer) {
                            auditDao.saveRecords(buffer.take(buffer.size))
                            buffer.clear()
                        }
                        buffer.add(record as AuditRecordInternal)
                    }

                }
                if (!isEnabled) {
                    break
                }
            }
        }
    }
}