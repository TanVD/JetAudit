package tanvd.audit.implementation.clickhouse.aorm

import tanvd.aorm.Database
import tanvd.aorm.DatabaseProperties
import tanvd.audit.utils.PropertyLoader

object AuditDatabaseProperties : DatabaseProperties() {
    override val name: String by lazy { "default" }

    override val url: String by lazy { PropertyLoader["Url"]!! }

    override val password: String by lazy { PropertyLoader["Password"]!! }
    override val user: String by lazy { PropertyLoader["Username"]!! }

    override val useSsl: Boolean by lazy { PropertyLoader["UseSSL"]?.toBoolean() ?: false }
    override val sslCertPath: String by lazy { PropertyLoader["SSLCertPath"] ?: "" }
    override val sslVerifyMode: String by lazy { PropertyLoader["SSLVerifyMode"] ?: "" }

    override val connectionTimeout: Int by lazy { PropertyLoader["ConnectionTimeout"]?.toInt() ?: 10000 }
    override val socketTimeout: Int by lazy { PropertyLoader["SocketTimeout"]?.toInt() ?: 30000 }

    override val keepAliveTimeout: Int by lazy { PropertyLoader["KeepAliveTimeout"]?.toInt() ?: 30000 }
    override val timeToLiveMillis: Int by lazy { PropertyLoader["TimeToLive"]?.toInt() ?: 60000 }

    override val dataTransferTimeout: Int by lazy { PropertyLoader["DataTransferTimeout"]?.toInt() ?: 20000 }

    override val maxTotalHttpThreads: Int by lazy { PropertyLoader["MaxTotalHttpThreads"]?.toInt() ?: 1000 }
    override val maxPerRouteHttpThreads: Int by lazy { PropertyLoader["MaxPerRouteTotalHttpThreads"]?.toInt() ?: 500 }


    override val maxIdle: Int by lazy { PropertyLoader["MaxIdleConnections"]?.toInt() ?: 30 }
    override val minIdle: Int by lazy { PropertyLoader["MinIdleConnections"]?.toInt() ?: 1 }
    override val maxTotal: Int by lazy { PropertyLoader["MaxTotalConnections"]?.toInt() ?: 60 }
    override val testOnBorrow: Boolean = true
    override val testWhileIdle: Boolean = true
    override val timeBetweenEvictionRunsMillis: Long by lazy { PropertyLoader["TimeBetweenEvictionRuns"]?.toLong() ?: 30000 }
}

object AuditDatabase : Database(AuditDatabaseProperties)