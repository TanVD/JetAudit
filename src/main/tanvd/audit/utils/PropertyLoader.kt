package tanvd.audit.utils

import org.slf4j.LoggerFactory
import tanvd.audit.AuditAPI
import tanvd.audit.model.external.properties.Config
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.util.*

/**
 * Util class for property loading.
 *
 * It will load path to file from "config" SystemProperty and then load properties from this file
 *
 * If property not found default values will be used.
 */
internal object PropertyLoader {

    private var config: Config? = null

    private var properties: Properties = Properties()

    private const val propertyFilePath = "audit.config"

    private val logger = LoggerFactory.getLogger(PropertyLoader::class.java)

    fun setConfig(config: Config) {
        this.config = config
    }

    /**
     * Loads property from file located on given path
     *
     * If property not found will reload SystemProperty with file path and file
     */
    fun loadProperty(propertyName: String): String? {
        if (config != null) {
            return config?.getProperty(propertyName)
        } else {
            if (!properties.containsKey(propertyName)) {
                reloadProperties()
            }
            val property = properties.getProperty(propertyName)
            if (property == null) {
                logger.info("Property $propertyName not found. Using default value.")
            }
            return property
        }
    }

    private fun reloadProperties() {
        val propertiesSystem = System.getProperties()
        val fileName = propertiesSystem.getProperty(propertyFilePath)
        if (fileName == null) {
            logger.info("System property with path to properties file not found. Using default values.")
            properties.clear()
        } else {
            loadPropertiesFromFile(fileName)
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

