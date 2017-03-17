package tanvd.audit.implementation

import tanvd.audit.model.AuditRecord
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

/**
 * Starts audit saving workers
 */
class AuditExecutor(val auditQueue : LinkedBlockingQueue<AuditRecord>, numberOfExecutors: Int = 5) {
    val executorService : ExecutorService
    init {
        executorService = Executors.newFixedThreadPool(5)
        for (i in 1..numberOfExecutors) {
            executorService.execute {
                AuditWorker(auditQueue).start()
            }
        }
    }
}