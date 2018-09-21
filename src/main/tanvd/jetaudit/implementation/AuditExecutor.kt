package tanvd.jetaudit.implementation

import org.jetbrains.annotations.TestOnly
import tanvd.jetaudit.utils.Conf
import tanvd.jetaudit.utils.PropertyLoader
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Starts audit saving workers.
 *
 * AuditExecutor can report about workers state (working or not) and stop them, if necessary.
 */
internal class AuditExecutor(private val auditQueueInternal: BlockingQueue<QueueCommand>) {

    companion object Config {
        val numberOfWorkers by lazy { PropertyLoader[Conf.WORKERS_NUM].toInt() }
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

    /**
     * Stops all workers. Wait timeout to give them a time to save buffers.
     */
    fun stopWorkers(timeToWait: Long) {
        auditQueueInternal += workers.map { ShutDown() }
        Thread.sleep(timeToWait)
    }

    @TestOnly
    fun stillWorking(): Boolean =
            workers.any { it.buffer.isNotEmpty() || it.reserveBuffer.isNotEmpty() } || auditQueueInternal.isNotEmpty()
}