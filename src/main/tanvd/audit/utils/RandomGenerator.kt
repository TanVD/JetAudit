package tanvd.audit.utils

import java.math.BigInteger
import java.security.SecureRandom

object RandomGenerator {
    private val rnd = SecureRandom()

    fun next(len: Int = 10, radix: Int = 10): Long = BigInteger(128, rnd).toString(radix).take(len).toLong()
}
