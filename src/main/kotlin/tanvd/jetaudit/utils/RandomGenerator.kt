package tanvd.jetaudit.utils

import java.math.BigInteger
import java.security.SecureRandom

object RandomGenerator {
    private val rnd = SecureRandom()

    fun next(len: Int = 18): Long = BigInteger(128, rnd).toString(10).takeLast(len).toLong()
}
