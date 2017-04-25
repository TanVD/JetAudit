package tanvd.audit.utils

import java.math.BigInteger
import java.security.SecureRandom

object RandomGenerator {
    private val rnd = SecureRandom()

    val length: Int = 16

    fun next(len: Int = length, radix: Int = 10): Long {
        return BigInteger(128, rnd).toString(radix).take(len).toLong()
    }
}
