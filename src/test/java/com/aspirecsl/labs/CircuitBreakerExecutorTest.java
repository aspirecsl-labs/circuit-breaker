package com.aspirecsl.labs;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A unit test class for {@link CircuitBreakerExecutor}
 *
 * @author anoopr
 */
public class CircuitBreakerExecutorTest {

  private static final String TASK_ID = "SOME_TASK";

  @Test
  public void executesTaskWithoutBreakingTheCircuitIfThereAreNoExceptions() throws Exception {
    final CircuitBreakerExecutor<Boolean> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(TASK_ID, () -> true, 1);
    for (int i = 0; i < 5; i++) {
      circuitBreakerExecutor.execute();
    }
  }

  @Test
  public void executesTaskAfterFailuresSoLongAsFaultToleranceIsNotExceeded() {
    final int[] share = {0};
    final int[] result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    final CircuitBreakerExecutor<Integer> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(
            TASK_ID,
            () -> {
              if (share[0] > 0 && share[0] < 3) {
                throw new Exception();
              } else {
                return 1;
              }
            },
            3);
    for (int i = 0; i < 10; i++) {
      try {
        share[0] = i;
        result[i] = circuitBreakerExecutor.execute();
      } catch (Exception ignore) {
      }
    }

    assertThat(result).containsExactly(1, 0, 0, 1, 1, 1, 1, 1, 1, 1);
  }

  @Test
  public void doesNotExecuteTaskOnceTheCircuitIsBroken() {
    final int[] share = {0};
    final int[] result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    final CircuitBreakerExecutor<Integer> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(
            TASK_ID,
            () -> {
              if (share[0] > 0 && share[0] < 4) {
                throw new RuntimeException();
              } else {
                return 1;
              }
            },
            3);
    for (int i = 0; i < 10; i++) {
      try {
        share[0] = i;
        result[i] = circuitBreakerExecutor.execute();
      } catch (Exception ignore) {
      }
    }

    assertThat(result).containsExactly(1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  }

  @Test
  public void taskFaultCounterIsResetEveryTimeTheTaskPasses() {
    final int[] share = {0};
    final int[] result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    final CircuitBreakerExecutor<Integer> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(
            TASK_ID,
            () -> {
              if (share[0] % 2 == 0) {
                throw new RuntimeException();
              } else {
                return 1;
              }
            },
            3);
    for (int i = 0; i < 10; i++) {
      try {
        share[0] = i;
        result[i] = circuitBreakerExecutor.execute();
      } catch (Exception ignore) {
      }
    }

    assertThat(result).containsExactly(0, 1, 0, 1, 0, 1, 0, 1, 0, 1);
  }

  @Test
  public void executesVoidReturningTaskWithoutBreakingTheCircuitIfThereAreNoExceptions()
      throws Exception {
    final CircuitBreakerExecutor<Void> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(TASK_ID, () -> {}, 2);
    for (int i = 0; i < 5; i++) {
      circuitBreakerExecutor.execute();
    }
  }

  @Test
  public void executesVoidReturningTaskAfterFailuresSoLongAsFaultToleranceIsNotExceeded() {
    final int[] share = {0};
    final int[] result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    final CircuitBreakerExecutor<Void> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(
            TASK_ID,
            () -> {
              if (share[0] > 0 && share[0] < 4) {
                throw new RuntimeException();
              } else {
                result[share[0]] = 1;
              }
            },
            4);
    for (int i = 0; i < 10; i++) {
      try {
        share[0] = i;
        circuitBreakerExecutor.execute();
      } catch (Exception ignore) {
      }
    }

    assertThat(result).containsExactly(1, 0, 0, 0, 1, 1, 1, 1, 1, 1);
  }

  @Test
  public void voidReturningTaskFaultCounterIsResetEveryTimeTheTaskPasses() {
    final int[] share = {0};
    final int[] result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    final CircuitBreakerExecutor<Void> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(
            TASK_ID,
            () -> {
              if (share[0] % 2 == 0) {
                throw new RuntimeException();
              } else {
                result[share[0]] = 1;
              }
            },
            4);
    for (int i = 0; i < 10; i++) {
      try {
        share[0] = i;
        circuitBreakerExecutor.execute();
      } catch (Exception ignore) {
      }
    }

    assertThat(result).containsExactly(0, 1, 0, 1, 0, 1, 0, 1, 0, 1);
  }

  @Test
  public void doesNotExecuteVoidReturningTaskOnceTheCircuitIsBroken() {
    final int[] share = {0};
    final int[] result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    final CircuitBreakerExecutor<Void> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(
            TASK_ID,
            () -> {
              if (share[0] > 0 && share[0] < 5) {
                throw new RuntimeException();
              } else {
                result[share[0]] = 1;
              }
            },
            4);
    for (int i = 0; i < 10; i++) {
      try {
        share[0] = i;
        circuitBreakerExecutor.execute();
      } catch (Exception ignore) {
      }
    }

    assertThat(result).containsExactly(1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  }

  @Test
  public void onlyBreaksTheCircuitForSpecifiedExceptions() {
    final int[] share = {0};
    final int[] result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    final CircuitBreakerExecutor<Integer> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(
            TASK_ID,
            () -> {
              if (share[0] % 2 == 0) {
                if (share[0] < 3) {
                  if (ThreadLocalRandom.current().nextInt(0, 2) > 0) {
                    throw new IllegalStateException();
                  } else {
                    throw new TimeoutException();
                  }
                } else {
                  throw new IllegalArgumentException();
                }
              } else {
                return 1;
              }
            },
            3,
            Arrays.asList(IllegalStateException.class, TimeoutException.class));
    for (int i = 0; i < 10; i++) {
      try {
        share[0] = i;
        result[i] = circuitBreakerExecutor.execute();
      } catch (Exception ignore) {
      }
    }

    assertThat(result).containsExactly(0, 1, 0, 1, 0, 1, 0, 1, 0, 1);
  }

  @Test
  public void onlyBreaksTheCircuitForSpecifiedExceptionsForVoidReturningTask() {
    final int[] share = {0};
    final int[] result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    final CircuitBreakerExecutor<Void> circuitBreakerExecutor =
        CircuitBreakerExecutor.create(
            TASK_ID,
            () -> {
              if (share[0] % 2 == 0) {
                if (share[0] < 3) {
                  if (ThreadLocalRandom.current().nextInt(0, 2) > 0) {
                    throw new IllegalStateException();
                  } else {
                    throw new IllegalArgumentException();
                  }
                } else {
                  throw new RuntimeException();
                }
              } else {
                result[share[0]] = 1;
              }
            },
            4,
            Arrays.asList(IllegalStateException.class, IllegalArgumentException.class));

    for (int i = 0; i < 10; i++) {
      try {
        share[0] = i;
        circuitBreakerExecutor.execute();
      } catch (Exception ignore) {
      }
    }

    assertThat(result).containsExactly(0, 1, 0, 1, 0, 1, 0, 1, 0, 1);
  }
}
