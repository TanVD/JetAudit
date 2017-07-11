package utils

import tanvd.audit.model.external.db.DbProperties

object DbUtils {
    fun getDbProperties(): DbProperties {
        val dbProperties = DbProperties()
        dbProperties.user = "default"
        dbProperties.password = ""
        dbProperties.connectionUrl = "jdbc:clickhouse://localhost:8123/example"
        return dbProperties
    }
}