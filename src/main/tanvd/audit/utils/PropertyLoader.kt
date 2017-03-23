package tanvd.audit.utils

import org.slf4j.LoggerFactory
import java.io.IOException

/** Util class for property load. */
object PropertyLoader {

    private val logger = LoggerFactory.getLogger(PropertyLoader::class.java)

    /** Loads property from property file located on a classpath. */
    fun load(fileName: String, propertyName: String): String {
        var result = ""
        val input = Thread.currentThread().contextClassLoader.getResourceAsStream(fileName)
        if (input == null) {
            logger.error("Property file {} not found", fileName)
            return result
        }
        try {
            val properties = java.util.Properties()
            properties.load(input)
            result = properties.getProperty(propertyName)
        } catch (e: IOException) {
            logger.error("Exception occurred during processing of $fileName properties file seeking" +
                    " for property $propertyName", e)
        } finally {
            try {
                input.close()
            } catch (e: IOException) {
                logger.error("Exception occurred during processing of $fileName properties file seeking " +
                        "for property $propertyName", e)
            }

        }
        return result
    }
}

