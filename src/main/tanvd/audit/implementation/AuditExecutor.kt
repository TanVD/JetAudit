package tanvd.audit.implementation

import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.utils.PropertyLoader
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Starts audit saving workers.
 *
 * AuditExecutor can report about workers state (working or not) and stop them, if necessary.
 */
internal class AuditExecutor(val auditQueueInternal: BlockingQueue<AuditRecordInternal>) {

    companion object Config {
        val numberOfWorkers by lazy { PropertyLoader.loadProperty("NumberOfWorkers")?.toInt() ?: 5 }
    }

    val workers: List<AuditWorker>

    val executorService: ExecutorService = Executors.newFixedThreadPool(numberOfWorkers)

    init {
        val workersList = ArrayList<AuditWorker>()
        for (i in 1..numberOfWorkers) {
            executorService.execute {
                val auditWorker = AuditWorker(auditQueueInternal)
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
}