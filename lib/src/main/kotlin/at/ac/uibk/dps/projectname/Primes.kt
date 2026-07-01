package at.ac.uibk.dps.projectname

object Primes {
  private tailrec fun mulMod(a: Long, b: Long, m: Long, r: Long = 0L): Long =
    if (b == 0L) r else mulMod((a shl 1) % m, b ushr 1, m, if (b and 1L != 0L) (r + a) % m else r)

  private tailrec fun modExp(b: Long, e: Long, m: Long, r: Long = 1L): Long =
    if (e == 0L) r
    else modExp(mulMod(b, b, m), e ushr 1, m, if (e and 1L != 0L) mulMod(r, b, m) else r)

  val isPrime: (Long) -> Boolean = { n ->
    when {
      n < 2L -> false
      n == 2L || n == 3L -> true
      n % 2L == 0L || n % 3L == 0L -> false
      else -> {
        var isP = true
        var i = 5L
        while (i * i <= n) {
          if (n % i == 0L || n % (i + 2L) == 0L) {
            isP = false
            break
          }
          i += 6L
        }
        isP
      }
    }
  }

  fun approximate() = sequence {
    yield(2L)
    var n = 3L
    while (true) {
      if (modExp(2L, n - 1, n) == 1L) yield(n)
      n += 2L
    }
  }
}
