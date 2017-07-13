package tanvd.audit.model.external.properties

interface Config {
    fun getProperty(key: String): String?
}