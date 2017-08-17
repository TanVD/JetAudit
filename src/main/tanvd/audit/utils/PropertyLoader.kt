package tanvd.audit.utils

import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.util.*


/**
 * Util class for property loading.
 *
 * You should set path to property explicitly before using.
 *
 * If property not found null will be returned and default value will be used.
 */
object PropertyLoader {

    private val logger = LoggerFactory.getLogger(PropertyLoader::class.java)

    private var instance: PropertyLoaderSingl? = null

    fun setPropertyFilePath(filePath: String) {
        instance = PropertyLoaderSingl(propertyFilePath = filePath)
    }

    fun setProperties(properties: Properties) {
        instance = PropertyLoaderSingl(null, properties)
    }

    operator fun get(propertyName: String): String? {
        if (instance == null) {
            logger.error("Property loader probably was not initialized. It may affect work of JetAudit")
            instance = PropertyLoaderSingl()
        }
        return PropertiesUtils.resolveEnvVariables(instance!!.loadProperty(propertyName))
    }

    private class PropertyLoaderSingl(val propertyFilePath: String? = null, val properties: Properties = Properties()) {
        /**
         * Loads property from file located on given path
         *
         * If property not found will reload SystemProperty with file path and file
         */
        fun loadProperty(propertyName: String): String? {
            if (!properties.containsKey(propertyName)) {
                reloadProperties()
            }
            val property = properties.getProperty(propertyName)
            if (property == null) {
                logger.info("Property $propertyName not found. Using default value.")
            }
            return property
        }


        private fun reloadProperties() {
            if (propertyFilePath == null) {
                logger.info("Path to properties file is null. Using default values.")
            } else {
                loadPropertiesFromFile(propertyFilePath)
            }
        }

        /**
         * Load property from property file located on a classpath.
         */
        private fun loadPropertiesFromFile(filePath: String) {
            val input: FileInputStream

            try {
                input = FileInputStream(filePath)
            } catch (e: FileNotFoundException) {
                logger.error("Property file $filePath not found")
                return
            }

            try {
                properties.clear()
                properties.load(input)
            } catch (e: IOException) {
                logger.error("Exception occurred during processing of $filePath properties file", e)
            } finally {
                try {
                    input.close()
                } catch (e: IOException) {
                    logger.error("Exception occurred during processing of $filePath properties file", e)
                }

            }
        }
    }
}

object PropertiesUtils {

    private val variable = Regex("\\$\\{(.+)\\}|\\$(.+)")

    fun resolveEnvVariables(str: String?): String? {
        return str?.replace(variable, { match ->
            System.getProperty(match.value.drop("\${".length).dropLast("}".length)) ?: match.value
        })
    }
}

