package org.slf4j

/** slf4j logging mixin convenience */
trait Logging {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  
  def trace(message: String) = logger.trace(message)
  def trace(format: String, arguments: Any*) = logger.trace(format, arguments.toArray)
  def trace(message: String, exception: Throwable) = logger.trace(message, exception)
  def trace(marker: Marker, message: String) = logger.trace(marker, message)
  def trace(marker: Marker, format: String, arguments: Any*) = logger.trace(marker, format, arguments.toArray)
  def trace(marker: Marker, message: String, exception: Throwable) = logger.trace(marker, message, exception)
  def isTraceEnabled = logger.isTraceEnabled
  def isTraceEnabled(marker: Marker) = logger.isTraceEnabled(marker)

  def debug(message: String) = logger.debug(message)
  def debug(format: String, arguments: Any*) = logger.debug(format, arguments.toArray)
  def debug(message: String, exception: Throwable) = logger.debug(message, exception)
  def debug(marker: Marker, message: String) = logger.debug(marker, message)
  def debug(marker: Marker, format: String, arguments: Any*) = logger.debug(marker, format, arguments.toArray)
  def debug(marker: Marker, message: String, exception: Throwable) = logger.debug(marker, message, exception)
  def isDebugEnabled = logger.isDebugEnabled
  def isDebugEnabled(marker: Marker) = logger.isDebugEnabled(marker)

  def info(message: String) = logger.info(message)
  def info(format: String, arguments: Any*) = logger.info(format, arguments.toArray)
  def info(message: String, exception: Throwable) = logger.info(message, exception)
  def info(marker: Marker, message: String) = logger.info(marker, message)
  def info(marker: Marker, format: String, arguments: Any*) = logger.info(marker, format, arguments.toArray)
  def info(marker: Marker, message: String, exception: Throwable) = logger.info(marker, message, exception)
  def isInfoEnabled = logger.isInfoEnabled
  def isInfoEnabled(marker: Marker) = logger.isInfoEnabled(marker)

  def warn(message: String) = logger.warn(message)
  def warn(format: String, arguments: Any*) = logger.warn(format, arguments.toArray)
  def warn(message: String, exception: Throwable) = logger.warn(message, exception)
  def warn(marker: Marker, message: String) = logger.warn(marker, message)
  def warn(marker: Marker, format: String, arguments: Any*) = logger.warn(marker, format, arguments.toArray)
  def warn(marker: Marker, message: String, exception: Throwable) = logger.warn(marker, message, exception)
  def isWarnEnabled = logger.isWarnEnabled
  def isWarnEnabled(marker: Marker) = logger.isWarnEnabled(marker)

  def error(message: String) = logger.error(message)
  def error(format: String, arguments: Any*) = logger.error(format, arguments.toArray)
  def error(message: String, exception: Throwable) = logger.error(message, exception)
  def error(marker: Marker, message: String) = logger.error(marker, message)
  def error(marker: Marker, format: String, arguments: Any*) = logger.error(marker, format, arguments.toArray)
  def error(marker: Marker, message: String, exception: Throwable) = logger.error(marker, message, exception)
  def isErrorEnabled = logger.isErrorEnabled
  def isErrorEnabled(marker: Marker) = logger.isErrorEnabled(marker)
}

/** slf4j logging import convenience
 * add import:
 * 		import org.slf4j.Log._
 * than log with:
 * 		<log-method>.(caller, ...)
 */
object Log {
//  def logger[A](caller: A):Logger = logger(caller.getClass)
//  def logger[A](`class`: Class[A]):Logger = logger(`class`.getName)
//  def logger(name: String):Logger = LoggerFactory.getLogger(name)
  def logger[A](caller: A) = LoggerFactory.getLogger(caller.getClass)
  def logger[A](caller: Class[A]) = LoggerFactory.getLogger(caller)
  def logger(name: String) = LoggerFactory.getLogger(name)

  def trace[A](caller: A, message: String) = logger(caller).trace(message)
  def trace[A](caller: A, format: String, arguments: Any*) = logger(caller).trace(format, arguments.toArray)
  def trace[A](caller: A, message: String, exception: Throwable) = logger(caller).trace(message, exception)
  def trace[A](caller: A, marker: Marker, message: String) = logger(caller).trace(marker, message)
  def trace[A](caller: A, marker: Marker, format: String, arguments: Any*) = logger(caller).trace(marker, format, arguments.toArray)
  def trace[A](caller: A, marker: Marker, message: String, exception: Throwable) = logger(caller).trace(marker, message, exception)
  def isTraceEnabled[A](caller: A) = logger(caller).isTraceEnabled
  def isTraceEnabled[A](caller: A, marker: Marker) = logger(caller).isTraceEnabled(marker)

  def debug[A](caller: A, message: String) = logger(caller).debug(message)
  def debug[A](caller: A, format: String, arguments: Any*) = logger(caller).debug(format, arguments.toArray)
  def debug[A](caller: A, message: String, exception: Throwable) = logger(caller).debug(message, exception)
  def debug[A](caller: A, marker: Marker, message: String) = logger(caller).debug(marker, message)
  def debug[A](caller: A, marker: Marker, format: String, arguments: Any*) = logger(caller).debug(marker, format, arguments.toArray)
  def debug[A](caller: A, marker: Marker, message: String, exception: Throwable) = logger(caller).debug(marker, message, exception)
  def isDebugEnabled[A](caller: A) = logger(caller).isDebugEnabled
  def isDebugEnabled[A](caller: A, marker: Marker) = logger(caller).isDebugEnabled(marker)

  def info[A](caller: A, message: String) = logger(caller).info(message)
  def info[A](caller: A, format: String, arguments: Any*) = logger(caller).info(format, arguments.toArray)
  def info[A](caller: A, message: String, exception: Throwable) = logger(caller).info(message, exception)
  def info[A](caller: A, marker: Marker, message: String) = logger(caller).info(marker, message)
  def info[A](caller: A, marker: Marker, format: String, arguments: Any*) = logger(caller).info(marker, format, arguments.toArray)
  def info[A](caller: A, marker: Marker, message: String, exception: Throwable) = logger(caller).info(marker, message, exception)
  def isInfoEnabled[A](caller: A) = logger(caller).isInfoEnabled
  def isInfoEnabled[A](caller: A, marker: Marker) = logger(caller).isInfoEnabled(marker)

  def warn[A](caller: A, message: String) = logger(caller).warn(message)
  def warn[A](caller: A, format: String, arguments: Any*) = logger(caller).warn(format, arguments.toArray)
  def warn[A](caller: A, message: String, exception: Throwable) = logger(caller).warn(message, exception)
  def warn[A](caller: A, marker: Marker, message: String) = logger(caller).warn(marker, message)
  def warn[A](caller: A, marker: Marker, format: String, arguments: Any*) = logger(caller).warn(marker, format, arguments.toArray)
  def warn[A](caller: A, marker: Marker, message: String, exception: Throwable) = logger(caller).warn(marker, message, exception)
  def isWarnEnabled[A](caller: A) = logger(caller).isWarnEnabled
  def isWarnEnabled[A](caller: A, marker: Marker) = logger(caller).isWarnEnabled(marker)

  def error[A](caller: A, message: String) = logger(caller).error(message)
  def error[A](caller: A, format: String, arguments: Any*) = logger(caller).error(format, arguments.toArray)
  def error[A](caller: A, message: String, exception: Throwable) = logger(caller).error(message, exception)
  def error[A](caller: A, marker: Marker, message: String) = logger(caller).error(marker, message)
  def error[A](caller: A, marker: Marker, format: String, arguments: Any*) = logger(caller).error(marker, format, arguments.toArray)
  def error[A](caller: A, marker: Marker, message: String, exception: Throwable) = logger(caller).error(marker, message, exception)
  def isErrorEnabled[A](caller: A) = logger(caller).isErrorEnabled
  def isErrorEnabled[A](caller: A, marker: Marker) = logger(caller).isErrorEnabled(marker)
}
