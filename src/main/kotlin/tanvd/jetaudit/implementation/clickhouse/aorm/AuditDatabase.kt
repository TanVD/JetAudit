package tanvd.jetaudit.implementation.clickhouse.aorm

import tanvd.aorm.Database
import tanvd.aorm.context.ConnectionContext
import tanvd.aorm.withDatabase
import javax.sql.DataSource

object AuditDatabase {
    lateinit var database: Database

    fun init(name: String, dataSource: DataSource) {
        database = Database(name, dataSource)
    }
}

fun <T> withAuditDatabase(body: ConnectionContext.() -> T): T = withDatabase(AuditDatabase.database) {
    body()
}
