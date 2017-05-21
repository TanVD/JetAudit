package tanvd.audit.model.external.db

import javax.sql.DataSource

class DbProperties {

    /**
     * Using of default DDL requests.
     *
     * Creating table, columns for new types and etc. Disable it if you
     * have complex structure. (Like replicated tables).
     */
    var useDefaultDDL = true

    var dataSource: DataSource? = null

    var connectionUrl: String? = null
    var user: String? = null
    var password: String? = null

    fun isDataSourceSet(): Boolean {
        return dataSource != null
    }

    fun isCredentialsSet(): Boolean {
        return (connectionUrl != null) && (user != null) && (password != null)
    }


}