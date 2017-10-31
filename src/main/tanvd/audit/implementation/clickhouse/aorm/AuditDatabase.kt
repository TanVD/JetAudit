package tanvd.audit.implementation.clickhouse.aorm

import tanvd.aorm.Database
import tanvd.audit.utils.PropertyLoader

object AuditDatabase : Database() {
    override val url: String by lazy { PropertyLoader["Url"]!! }
    override val password: String by lazy { PropertyLoader["Password"]!! }
    override val user: String by lazy { PropertyLoader["Username"]!! }

    override val useSsl: Boolean by lazy { PropertyLoader["UseSSL"]?.toBoolean() ?: false }
    override val sslCertPath: String by lazy { PropertyLoader["SSLCertPath"] ?: "" }
    override val sslVerifyMode: String by lazy { PropertyLoader["SSLVerifyMode"] ?: "" }
}