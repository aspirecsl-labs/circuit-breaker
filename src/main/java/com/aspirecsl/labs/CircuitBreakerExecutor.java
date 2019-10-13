package com.aspirecsl.labs;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a <tt>circuit breaker</tt> interface to execute complex and/or time consuming tasks.
 *
 * <p>This executor will not execute the task again if it had failed a preset number of times with
 * any of the specified <tt>exceptions</tt>, or any <tt>RuntimeException</tt> <em>(if no exceptions
 * were specified)</em>.
 *
 * @author anoopr
 */
public class CircuitBreakerExecutor<T> {

  /** the label or description corresponding to the task * */
  private final String taskId;

  /** the complex and/or time consuming task to execute * */
  private final Callable<T> task;

  /** the maximum number of errors that will <em>trip</em> this execution wrapper * */
  private final int errorToleranceFactor;

  /** a running counter of the errors occurred while executing the task * */
  private final AtomicInteger errorCounter;

  /**
   * A list of exceptions (including <em>checked</em>) which when thrown by the task will cause it
   * to be deemed as erroneous.
   *
   * <p>If this list is empty then any <tt>RuntimeException</tt> thrown by the task will cause it to
   * be deemed as erroneous.
   */
  private final List<Class<? extends Exception>> failOnExceptions;

  /**
   * Constructs an instance with the supplied values if they are valid.
   *
   * <p>Input parameters are validated as:
   *
   * <ul>
   *   <li><tt>taskId</tt> is not <tt>null</tt>, <tt>empty</tt> or <tt>blank <em>(contains only
   *       whitespaces)</em></tt>
   *   <li><tt>task</tt> is not <tt>null</tt>
   *   <li><tt>errorToleranceFactor</tt> is not <tt>negative</tt>
   *   <li><tt>failOnExceptions</tt> is not <tt>null</tt>
   * </ul>
   *
   * @param taskId the label or description corresponding to the task
   * @param task the complex and/or time consuming task to execute
   * @param errorToleranceFactor the maximum number of errors that will <em>trip</em> this execution
   *     wrapper
   * @param failOnExceptions list of exceptions (including <em>checked</em>) which when thrown by
   *     the task will cause it to be deemed as erroneous. If <tt>empty</tt> then any
   *     <tt>RuntimeException</tt> thrown by the task will cause it to be deemed as erroneous.
   */
  private CircuitBreakerExecutor(
      String taskId,
      Callable<T> task,
      int errorToleranceFactor,
      List<Class<? extends Exception>> failOnExceptions) {

    validate(taskId, task, errorToleranceFactor, failOnExceptions);

    this.task = task;
    this.taskId = taskId;
    this.failOnExceptions = failOnExceptions;
    this.errorToleranceFactor = errorToleranceFactor;

    errorCounter = new AtomicInteger(0);
  }

  /**
   * Validates the input parameters as:
   *
   * <ul>
   *   <li><tt>taskId</tt> is not <tt>null</tt>, <tt>empty</tt> or <tt>blank <em>(contains only
   *       whitespaces)</em></tt>
   *   <li><tt>task</tt> is not <tt>null</tt>
   *   <li><tt>errorToleranceFactor</tt> is not <tt>negative</tt>
   *   <li><tt>failOnExceptions</tt> is not <tt>null</tt>
   * </ul>
   *
   * @param taskId the label or description corresponding to the task
   * @param task the complex and/or time consuming task to execute
   * @param errorToleranceFactor the maximum number of errors that will <em>trip</em> this execution
   *     wrapper
   * @param failOnExceptions list of exceptions (including <em>checked</em>) which when thrown by
   *     the task will cause it to be deemed as erroneous. If <tt>empty</tt> then any
   *     <tt>RuntimeException</tt> thrown by the task will cause it to be deemed as erroneous.
   * @throws NullPointerException if any of the specified <tt>task</tt>, <tt>taskId</tt> or
   *     <tt>failOnExceptions</tt> is <tt>null</tt>
   * @throws IllegalArgumentException if the specified <tt>taskId</tt> is empty or the
   *     <tt>errorToleranceFactor</tt> is < 0
   */
  private void validate(
      String taskId,
      Callable<T> task,
      int errorToleranceFactor,
      List<Class<? extends Exception>> failOnExceptions) {

    Objects.requireNonNull(task);
    Objects.requireNonNull(taskId);
    Objects.requireNonNull(failOnExceptions);
    if (taskId.isEmpty()) {
      throw new IllegalArgumentException("Task id must not be empty");
    }
    if (errorToleranceFactor <= 0) {
      throw new IllegalArgumentException("Error tolerance factor must be > 0");
    }
  }

  /**
   * Returns a <tt>CircuitBreakerExecutor</tt> instance with the supplied values.
   *
   * <p>This <tt>CircuitBreakerExecutor</tt> will deem a task to be erroneous if it throws a
   * <tt>RuntimeException</tt>
   *
   * @param taskId the label or description corresponding to the task
   * @param task the complex and/or time consuming <tt>Callable</tt> task to execute
   * @param errorToleranceFactor the maximum number of errors that will <em>trip</em> this execution
   *     wrapper
   * @return a <tt>CircuitBreakerExecutor</tt> instance with the supplied values
   */
  public static <T> CircuitBreakerExecutor<T> create(
      String taskId, Callable<T> task, int errorToleranceFactor) {
    return create(taskId, task, errorToleranceFactor, Collections.emptyList());
  }

  /**
   * Returns a <tt>CircuitBreakerExecutor</tt> instance with the supplied values.
   *
   * <p>This <tt>CircuitBreakerExecutor</tt> will deem a task to be erroneous if it throws a
   * <tt>RuntimeException</tt>
   *
   * @param taskId the label or description corresponding to the task
   * @param task the complex and/or time consuming <tt>Runnable</tt> task to execute
   * @param errorToleranceFactor the maximum number of errors that will <em>trip</em> this execution
   *     wrapper
   * @return a <tt>CircuitBreakerExecutor</tt> instance with the supplied values
   */
  public static CircuitBreakerExecutor<Void> create(
      String taskId, Runnable task, int errorToleranceFactor) {
    return create(taskId, task, errorToleranceFactor, Collections.emptyList());
  }

  /**
   * Returns a <tt>CircuitBreakerExecutor</tt> instance with the supplied values.
   *
   * @param taskId the label or description corresponding to the task
   * @param task the complex and/or time consuming <tt>Runnable</tt> task to execute
   * @param errorToleranceFactor the maximum number of errors that will <em>trip</em> this execution
   *     wrapper
   * @param failOnExceptions list of exceptions (including <em>checked</em>) which when thrown by
   *     the task will cause it to be deemed as erroneous. If <tt>empty</tt> then any
   *     <tt>RuntimeException</tt> thrown by the task will cause it to be deemed as erroneous.
   * @return a <tt>CircuitBreakerExecutor</tt> instance with the supplied values
   */
  public static <T> CircuitBreakerExecutor<T> create(
      String taskId,
      Callable<T> task,
      int errorToleranceFactor,
      List<Class<? extends Exception>> failOnExceptions) {
    return new CircuitBreakerExecutor<>(taskId, task, errorToleranceFactor, failOnExceptions);
  }

  /**
   * Returns a <tt>CircuitBreakerExecutor</tt> instance with the supplied values.
   *
   * @param taskId the label or description corresponding to the task
   * @param task the complex and/or time consuming <tt>Runnable</tt> task to execute
   * @param errorToleranceFactor the maximum number of errors that will <em>trip</em> this execution
   *     wrapper
   * @param failOnExceptions list of exceptions (including <em>checked</em>) which when thrown by
   *     the task will cause it to be deemed as erroneous. If <tt>empty</tt> then any
   *     <tt>RuntimeException</tt> thrown by the task will cause it to be deemed as erroneous.
   * @return a <tt>CircuitBreakerExecutor</tt> instance with the supplied values
   */
  public static CircuitBreakerExecutor<Void> create(
      String taskId,
      Runnable task,
      int errorToleranceFactor,
      List<Class<? extends Exception>> failOnExceptions) {
    final Callable<Void> callable =
        () -> {
          task.run();
          return null;
        };
    return new CircuitBreakerExecutor<>(taskId, callable, errorToleranceFactor, failOnExceptions);
  }

  /**
   * Executes the <tt>task</tt> in this <tt>CircuitBreakerExecutor</tt> instance
   *
   * @return the result obtained by executing the <tt>task</tt>
   * @throws Exception if the <tt>task</tt> throws an exception
   * @throws RuntimeException if this <tt>CircuitBreakerExecutor</tt> instance has already
   *     <em>tripped</em>, meaning the running <tt>errorCounter</tt> is equal to the
   *     <tt>errorToleranceFactor</tt>
   */
  public T execute() throws Exception {
    if (errorCounter.get() == errorToleranceFactor) {
      throw new RuntimeException("Too many failures for task [" + taskId + "].");
    }
    try {
      final T result = task.call();
      errorCounter.set(0);
      return result;
    } catch (Exception e) {
      if ((failOnExceptions.isEmpty() && RuntimeException.class.isAssignableFrom(e.getClass()))
          || failOnExceptions.contains(e.getClass())) {
        errorCounter.incrementAndGet();
      }
      throw e;
    }
  }
}
