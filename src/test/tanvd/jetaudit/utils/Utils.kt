package tanvd.jetaudit.utils


fun measureTime(func: () -> Unit): Long {
    val time = System.currentTimeMillis()
    func()
    return System.currentTimeMillis() - time
}

fun waitUntilRightCount(func: () -> Boolean, sleepTime: Long, totalNumber: Long) {
    var current = 0L
    while (!func()) {
        if (current == totalNumber) {
            return
        }
        Thread.sleep(sleepTime)
        current++
    }
}