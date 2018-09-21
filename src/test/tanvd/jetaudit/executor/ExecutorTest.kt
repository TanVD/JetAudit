package tanvd.jetaudit.executor

import org.mockito.Mockito.*
import org.powermock.api.mockito.PowerMockito
import org.powermock.api.mockito.PowerMockito.mockStatic
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.testng.PowerMockTestCase
import org.testng.Assert
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.jetaudit.implementation.AuditExecutor
import tanvd.jetaudit.implementation.AuditWorker
import tanvd.jetaudit.implementation.QueueCommand
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(fullyQualifiedNames = ["tanvd.jetaudit.implementation.*"])
internal class ExecutorTest : PowerMockTestCase() {

    private var auditWorker: AuditWorker? = null

    private var executorService: ExecutorService? = null

    private val queue = ArrayBlockingQueue<QueueCommand>(1)


    @BeforeMethod
    fun setMocks() {
        //mock audit worker
        auditWorker = PowerMockito.mock(AuditWorker::class.java)
        //mock executor server to work synchronous
        executorService = PowerMockito.mock(ExecutorService::class.java)
        doAnswer { invocation ->
            (invocation.arguments[0] as Runnable).run()
        }.`when`(executorService)!!.execute(any(Runnable::class.java))
        //mock ThreadPool to return mocked executor service
        mockStatic(Executors::class.java)
        PowerMockito.`when`(Executors.newFixedThreadPool(AuditExecutor.numberOfWorkers)).thenReturn(executorService)
        //mock audit worker to create on new mocked version
        PowerMockito.whenNew(AuditWorker::class.java).withArguments(queue).thenReturn(auditWorker)
    }

    @AfterMethod
    fun resetMocks() {
        reset(auditWorker)
        reset(executorService)
    }

    @Test
    fun init_defaultNumberOfWorkers_allWorkersStarted() {
        AuditExecutor(queue)

        verify(auditWorker, times(AuditExecutor.numberOfWorkers))!!.start()
    }

    @Test
    fun init_defaultNumberOfWorkers_allWorkersAddedToQueue() {
        val auditExecutor = AuditExecutor(queue)

        Assert.assertEquals(auditExecutor.workers.size, AuditExecutor.numberOfWorkers)
        for (worker in auditExecutor.workers) {
            Assert.assertEquals(worker, auditWorker)
        }
    }

}
