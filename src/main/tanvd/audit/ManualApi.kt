package tanvd.audit

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.audit.implementation.clickhouse.AuditDaoClickhouseImpl
import tanvd.audit.model.external.types.InnerType
import tanvd.audit.model.external.types.information.InformationType
import tanvd.audit.model.external.types.objects.ObjectType
import java.util.*

internal object ManualApi {

    init {
        InformationType.apply {
            addType(InformationType(IdPresenter, InnerType.Long) as InformationType<Any>)
            addType(InformationType(VersionPresenter, InnerType.ULong) as InformationType<Any>)
            addType(InformationType(TimeStampPresenter, InnerType.Long) as InformationType<Any>)
            addType(InformationType(DatePresenter, InnerType.Date) as InformationType<Any>)
            addType(InformationType(IsDeletedPresenter, InnerType.Boolean) as InformationType<Any>)
        }

        ObjectType.apply {
            addType(ObjectType(String::class, StringPresenter) as ObjectType<Any>)
            addType(ObjectType(Int::class, IntPresenter) as ObjectType<Any>)
            addType(ObjectType(Long::class, LongPresenter) as ObjectType<Any>)
        }


    }

    val username = ""
    val password = ""
    val url = ""
    val sslPath = ""
    val useSsl = true

    val dataSource by lazy {
        val properties = ClickHouseProperties()
        properties.user = username
        properties.password = password
        properties.connectionTimeout = 2000
        properties.timeToLiveMillis = 240000
        properties.keepAliveTimeout = 60000
        if (useSsl) {
            properties.ssl = true
            properties.sslRootCertificate = sslPath
            properties.sslMode = "strict"
        }
        ClickHouseDataSource(url, properties)
    }

    val jdbc = JdbcClickhouseConnection(dataSource)

    val dao = AuditDaoClickhouseImpl(dataSource)
}

fun main(args : Array<String>) {
    val header = DbTableHeader(listOf(
            DbColumnHeader(DatePresenter.code, DbColumnType.DbDate),
            DbColumnHeader(TimeStampPresenter.code, DbColumnType.DbLong),
            DbColumnHeader(IdPresenter.code, DbColumnType.DbLong),
            DbColumnHeader(VersionPresenter.code, DbColumnType.DbULong),
            DbColumnHeader(IsDeletedPresenter.code, DbColumnType.DbBoolean)));
    val rows = ManualApi.jdbc.loadRows(
            "SELECT DateColumn, TimeStampColumn, IdColumn, VersionColumn, IsDeletedColumn FROM AuditTable" +
                    " WHERE length(Description) = 1 and Description[1] = 'Account';\n", header)
    val newRows = ArrayList<DbRow>()
    for ((columns) in rows) {
        val newColumns = columns.map {
            when (it.name) {
                VersionPresenter.code -> {
                    DbColumn(DbColumnHeader(VersionPresenter.code, DbColumnType.DbULong), (it.elements[0].toLong() + 1).toString())
                }
                IsDeletedPresenter.code -> {
                    DbColumn(DbColumnHeader(IsDeletedPresenter.code, DbColumnType.DbBoolean), "true")
                }
                else -> {
                    it
                }
            }
        }
        newRows.add(DbRow(newColumns))
    }
    ManualApi.jdbc.insertRows("AuditTable", header, newRows)
    println(rows.size)
}