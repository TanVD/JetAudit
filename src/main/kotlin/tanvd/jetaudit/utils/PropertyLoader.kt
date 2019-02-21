package tanvd.jetaudit.utils

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

    private val overwriteProperties = Properties()

    private var instance: PropertyLoaderImpl? = null

    fun setPropertyFilePath(filePath: String) {
        if (instance == null) {
            instance = PropertyLoaderImpl(propertyFilePath = filePath)
        }
    }

    fun setProperties(properties: Properties) {
        if (instance == null) {
            instance = PropertyLoaderImpl(null, properties)
        }
    }

    fun setOverwrite(properties: Properties) {
        overwriteProperties += properties
    }

    operator fun get(property: Conf) = tryGet(property)
            ?: error("Can't resolve configuration property ${property.paramName}")

    fun tryGet(property: Conf): String? {
        val propertyName = property.propertyName()
        if (instance == null) {
            logger.error("Property loader probably was not initialized. It may affect work of JetAudit")
            instance = PropertyLoaderImpl()
        }
        return overwriteProperties[propertyName]?.toString()
                ?: PropertiesUtils.resolveEnvVariables(instance!!.loadProperty(propertyName))
                ?: property.defaultValue
    }

    private class PropertyLoaderImpl(val propertyFilePath: String? = null, val properties: Properties = Properties()) {
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
            val input: FileInputStream = try {
                FileInputStream(filePath)
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
        return str?.replace(variable) { match ->
            System.getProperty(match.value.drop("\${".length).dropLast("}".length)) ?: match.value
        }
    }
}

