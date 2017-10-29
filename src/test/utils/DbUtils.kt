package utils

import tanvd.aorm.Database
import java.util.*

object TestDatabase : Database(){
    override val url: String = System.getProperty("ClickhouseUrl")?.trim('"') ?: "jdbc:clickhouse://localhost:8123"
    override val password: String = ""
    override val user: String = "default"

}
object DbUtils {
    fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty("Username", "default")
        properties.setProperty("Password", "")
        properties.setProperty("Url", System.getProperty("ClickhouseUrl")?.trim('"') ?: "jdbc:clickhouse://localhost:8123")
        return properties
    }
}