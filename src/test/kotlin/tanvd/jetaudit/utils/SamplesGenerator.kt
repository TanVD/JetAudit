package tanvd.jetaudit.utils

import tanvd.jetaudit.model.external.records.InformationObject
import tanvd.jetaudit.model.external.records.ObjectState
import tanvd.jetaudit.model.external.types.objects.ObjectType
import tanvd.jetaudit.model.internal.AuditRecordInternal
import java.math.BigInteger
import java.security.SecureRandom
import java.util.*


internal object SamplesGenerator {

    private val random = SecureRandom()

    fun getRecordInternal(vararg obj: Any, information: LinkedHashSet<InformationObject<*>>): AuditRecordInternal {
        val listObjects = ArrayList<Pair<ObjectType<*>, ObjectState>>()
        for (o in obj) {
            val type = ObjectType.resolveType(o::class)
            listObjects.add(type to type.serialize(o))
        }
        return AuditRecordInternal(listObjects, information)
    }


    fun getRandomInt(bound: Int = Int.MAX_VALUE): Int {
        return random.nextInt(bound)
    }

    fun getRandomString(length: Int = 32): String {
        return BigInteger(130, random).toString(length)
    }


    fun getMillenniumStart(): Date {
        //start of year 2000
        return getDate("2000-01-01")
    }
}