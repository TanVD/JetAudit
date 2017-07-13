package tanvd.audit.implementation.dao

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.audit.exceptions.UninitializedException
import tanvd.audit.exceptions.UnknownObjectTypeException
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.implementation.exceptions.BasicDbException
import tanvd.audit.model.external.queries.QueryExpression
import tanvd.audit.model.external.queries.QueryParameters
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import tanvd.audit.model.internal.AuditRecordInternal
import tanvd.audit.model.internal.db.DbCredentials
import tanvd.audit.utils.PropertyLoader
import javax.sql.DataSource


internal interface AuditDao {

    /**
     * Saves specified record in Db or throws exception.
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun saveRecord(auditRecordInternal: AuditRecordInternal)

    /**
     * Saves specified records in Db or throws exception.
     *
     * In most cases bath saving works faster than for loop.
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun saveRecords(auditRecordInternals: List<AuditRecordInternal>)

    /**
     * Performs operations on Db scheme needed to support new audit type.
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun <T : Any> addTypeInDbModel(type: ObjectType<T>)

    /**
     * Performs operations on Db scheme needed to support new information type.
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun <T> addInformationInDbModel(information: InformationType<T>)

    /**
     * Load records satisfying expression limited by specified parameters.
     *
     * Implementations must pass parameters of query to Db (it means, that implementations must not apply parameters
     * to result set of query returned by Db)
     *
     * @throws UnknownObjectTypeException
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun loadRecords(expression: QueryExpression, parameters: QueryParameters): List<AuditRecordInternal>

    /**
     * Return number of records satisfying expression.
     *
     * Implementations must implement it with built in Db functions. (it means, that implementations must not count
     * number of records in result set of query returned by Db)
     *
     * @throws BasicDbException
     */
    @Throws(BasicDbException::class)
    fun countRecords(expression: QueryExpression): Int

    companion object AuditDaoFactory {

        var credentials: DbCredentials? = null

        var dataSource: DataSource? = null


        @Throws(UninitializedException::class)
        fun getDao(): AuditDao {
            if (dataSource != null) {
                return AuditDaoClickhouseImpl(dataSource!!)
            } else if (credentials != null) {
                val properties = ClickHouseProperties()
                properties.user = credentials!!.username
                properties.password = credentials!!.password
                properties.connectionTimeout = (PropertyLoader["Timeout"]?.toInt() ?: 10000)
                return AuditDaoClickhouseImpl(ClickHouseDataSource(credentials!!.url, properties))
            }
            throw UninitializedException("Nor credentials nor datasource set in DbProperties")
        }

    }

}