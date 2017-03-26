package tanvd.audit.implementation

import tanvd.audit.implementation.dao.AuditDao
import tanvd.audit.model.internal.AuditRecord
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Starts audit saving workers
 */
internal class AuditExecutor(val auditQueue: BlockingQueue<AuditRecord>, numberOfExecutors: Int = 5) {

    val workers: MutableList<AuditWorker> = ArrayList()

    val executorService: ExecutorService = Executors.newFixedThreadPool(numberOfExecutors)

    init {
        for (i in 1..numberOfExecutors) {
            executorService.execute {
                val auditWorker = AuditWorker(auditQueue)
                workers.add(auditWorker)
                auditWorker.start()
            }
        }
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
    class AuditWorker(val auditQueue: BlockingQueue<AuditRecord>, val maxBuffer: Int = 5000,
                      val timeToWait: Long = 10) {

        val buffer: MutableList<AuditRecord> = ArrayList()

        val auditDao: AuditDao = AuditDao.getDao()

        var isWorking: Boolean = true

        var isEnabled = true

        //TODO Check if reconnection too slow
        fun start() {
            while (true) {
                synchronized(isWorking) {
                    val record = auditQueue.poll(timeToWait, TimeUnit.MILLISECONDS)
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
                        buffer.add(record as AuditRecord)
                    }

                }
                if (!isEnabled) {
                    break
                }
            }
        }
    }
}