package at.ac.uibk.dps.projectname

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class PrimesTest {
  @Test
  fun `isPrime evaluates boundary and edge cases correctly`() {
    assertFalse(Primes.isPrime(-5L))
    assertFalse(Primes.isPrime(0L))
    assertFalse(Primes.isPrime(1L))
    assertTrue(Primes.isPrime(2L))
    assertTrue(Primes.isPrime(3L))
  }

  @Test
  fun `isPrime evaluates composites efficiently`() {
    assertFalse(Primes.isPrime(4L))
    assertFalse(Primes.isPrime(9L))
    assertFalse(Primes.isPrime(25L))
    assertFalse(Primes.isPrime(49L))
    assertFalse(Primes.isPrime(341L))
  }

  @Test
  fun `isPrime evaluates true primes accurately`() {
    assertTrue(Primes.isPrime(5L))
    assertTrue(Primes.isPrime(7L))
    assertTrue(Primes.isPrime(11L))
    assertTrue(Primes.isPrime(97L))
    assertTrue(Primes.isPrime(229L))
  }

  @Test
  fun `approximate yields true primes for the first 50 iterations`() {
    val approximations = Primes.approximate().take(50).toList()

    assertEquals(50, approximations.size)
    assertTrue(approximations.all(Primes.isPrime))
  }

  @Test
  fun `approximate mathematically diverges at the 69th element`() {
    val approximations = Primes.approximate().take(69).toList()
    val pseudoprime = approximations[68]

    assertEquals(341L, pseudoprime)
    assertFalse(Primes.isPrime(pseudoprime))
  }
}
