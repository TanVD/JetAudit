package tanvd.jetaudit.utils

import org.junit.Assert.*
import org.junit.Test

class RandomGeneratorTest {

    @Test
    fun testUniqueness() {
        val randoms = hashSetOf<Long>()
        repeat(1_000_000) {
            val r = RandomGenerator.next()
            assertFalse(r in randoms)
            randoms.add(r)
        }
    }
}