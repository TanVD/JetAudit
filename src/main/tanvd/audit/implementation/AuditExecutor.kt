package tanvd.audit.implementation

import tanvd.audit.model.AuditRecord
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

/**
 * Starts audit saving workers
 */
class AuditExecutor(val auditQueue : LinkedBlockingQueue<AuditRecord>,
                    numberOfExecutors: Int = 5) {

    val workers : MutableList<AuditWorker> = ArrayList()

    val executorService : ExecutorService = Executors.newFixedThreadPool(numberOfExecutors)

    init {
        for (i in 1..numberOfExecutors) {
            executorService.execute {
                val auditWorker = AuditWorker(auditQueue)
                workers.add(auditWorker)
                auditWorker.start()
            }
        }
    }

    fun isStillWorking() : Boolean {
        return workers.any { it.isWorking }
    }
}