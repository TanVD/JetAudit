package tanvd.audit.utils

import org.slf4j.LoggerFactory
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
object PropertyLoader {

    private var properties: Properties = Properties()

    private var propertyFilePath: String? = null

    private val logger = LoggerFactory.getLogger(PropertyLoader::class.java)

    fun setPropertyFilePath(filePath: String) {
        propertyFilePath = filePath
    }

    fun setProperties(properties: Properties) {
        this.properties = properties
    }

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

    operator fun get(propertyName: String): String? {
        return this.loadProperty(propertyName)
    }

    private fun reloadProperties() {
        if (propertyFilePath == null) {
            logger.info("Path to properties file is null. Using default values.")
            properties.clear()
        } else {
            loadPropertiesFromFile(propertyFilePath!!)
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

