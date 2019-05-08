package tanvd.jetaudit.executor

import org.junit.*
import org.junit.runner.RunWith
import org.mockito.Mockito.*
import org.powermock.api.mockito.PowerMockito
import org.powermock.api.mockito.PowerMockito.doAnswer
import org.powermock.api.mockito.PowerMockito.mockStatic
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import tanvd.jetaudit.implementation.*
import java.util.concurrent.*

@RunWith(PowerMockRunner::class)
@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "javax.net.ssl.*",
        "jdk.*",
        "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*",
        "org.slf4j.*")
@PrepareForTest(fullyQualifiedNames = ["tanvd.jetaudit.implementation.*"])
internal class ExecutorTest {

    private var auditWorker: AuditWorker? = null

    private var executorService: ExecutorService? = null

    private val queue = ArrayBlockingQueue<QueueCommand>(1)


    @Before
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

    @After
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
