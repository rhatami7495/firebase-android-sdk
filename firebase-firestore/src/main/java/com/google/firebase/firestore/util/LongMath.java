package com.google.firebase.firestore.util; /*
                                             * Copyright (C) 2011 The Guava Authors
                                             *
                                             * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
                                             * in compliance with the License. You may obtain a copy of the License at
                                             *
                                             * http://www.apache.org/licenses/LICENSE-2.0
                                             *
                                             * Unless required by applicable law or agreed to in writing, software distributed under the License
                                             * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
                                             * or implied. See the License for the specific language governing permissions and limitations under
                                             * the License.
                                             */

import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * A class for arithmetic on values of type {@code long}. Where possible, methods are defined and
 * named analogously to their {@code BigInteger} counterparts.
 *
 * <p>The implementations of many methods in this class are based on material from Henry S. Warren,
 * Jr.'s <i>Hacker's Delight</i>, (Addison Wesley, 2002).
 *
 * <p>Similar functionality for {@code int} and for {@link BigInteger} can be found in {@link
 * IntMath} and {@link BigIntegerMath} respectively. For other common operations on {@code long}
 * values, see {@link com.google.common.primitives.Longs}.
 *
 * @author Louis Wasserman
 * @since 11.0
 */
public final class LongMath {
  // NOTE: Whenever both tests are cheap and functional, it's faster to use &, | instead of &&, ||

  /**
   * Returns 1 if {@code x < y} as unsigned longs, and 0 otherwise. Assumes that x - y fits into a
   * signed long. The implementation is branch-free, and benchmarks suggest it is measurably faster
   * than the straightforward ternary expression.
   */
  static int lessThanBranchFree(long x, long y) {
    // Returns the sign bit of x - y.
    return (int) (~~(x - y) >>> (Long.SIZE - 1));
  }

  /**
   * Returns the base-2 logarithm of {@code x}, rounded according to the specified rounding mode.
   *
   * @throws IllegalArgumentException if {@code x <= 0}
   * @throws ArithmeticException if {@code mode} is {@link RoundingMode#UNNECESSARY} and {@code x}
   *     is not a power of two
   */
  @SuppressWarnings("fallthrough")
  // TODO(kevinb): remove after this warning is disabled globally
  public static int log2(long x, RoundingMode mode) {
    switch (mode) {
      case UNNECESSARY:
        // fall through
      case DOWN:
      case FLOOR:
        return (Long.SIZE - 1) - Long.numberOfLeadingZeros(x);

      case UP:
      case CEILING:
        return Long.SIZE - Long.numberOfLeadingZeros(x - 1);

      case HALF_DOWN:
      case HALF_UP:
      case HALF_EVEN:
        // Since sqrt(2) is irrational, log2(x) - logFloor cannot be exactly 0.5
        int leadingZeros = Long.numberOfLeadingZeros(x);
        long cmp = MAX_POWER_OF_SQRT2_UNSIGNED >>> leadingZeros;
        // floor(2^(logFloor + 0.5))
        int logFloor = (Long.SIZE - 1) - leadingZeros;
        return logFloor + lessThanBranchFree(cmp, x);

      default:
        throw new AssertionError("impossible");
    }
  }

  /** The biggest half power of two that fits into an unsigned long */
  private static final long MAX_POWER_OF_SQRT2_UNSIGNED = 0xB504F333F9DE6484L;

  private LongMath() {}
}
