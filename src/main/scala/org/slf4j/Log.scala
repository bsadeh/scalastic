package org.slf4j

object Log {
  def logger[T](caller: T) = LoggerFactory.getLogger(caller.getClass)
  def logger[T](caller: Class[T]) = LoggerFactory.getLogger(caller)
  def logger(name: String) = LoggerFactory.getLogger(name)

  def debug[T](caller: T, message: String) = logger(caller).debug(message)
  def debug[T](caller: T, format: String, arguments: Object*) = logger(caller).debug(format, arguments.toArray)
  def debug[T](caller: T, message: String, exception: Throwable) = logger(caller).debug(message, exception)
  def debug[T](caller: T, marker: Marker, message: String) = logger(caller).debug(marker, message)
  def debug[T](caller: T, marker: Marker, format: String, arguments: Object*) = logger(caller).debug(marker, format, arguments.toArray)
  def debug[T](caller: T, marker: Marker, message: String, exception: Throwable) = logger(caller).debug(marker, message, exception)
  def isDebugEnabled[T](caller: T) = logger(caller).isDebugEnabled
  def isDebugEnabled[T](caller: T, marker: Marker) = logger(caller).isDebugEnabled(marker)

  def error[T](caller: T, message: String) = logger(caller).error(message)
  def error[T](caller: T, format: String, arguments: Object*) = logger(caller).error(format, arguments.toArray)
  def error[T](caller: T, message: String, exception: Throwable) = logger(caller).error(message, exception)
  def error[T](caller: T, marker: Marker, message: String) = logger(caller).error(marker, message)
  def error[T](caller: T, marker: Marker, format: String, arguments: Object*) = logger(caller).error(marker, format, arguments.toArray)
  def error[T](caller: T, marker: Marker, message: String, exception: Throwable) = logger(caller).error(marker, message, exception)
  def isErrorEnabled[T](caller: T) = logger(caller).isErrorEnabled
  def isErrorEnabled[T](caller: T, marker: Marker) = logger(caller).isErrorEnabled(marker)

  def info[T](caller: T, message: String) = logger(caller).info(message)
  def info[T](caller: T, format: String, arguments: Object*) = logger(caller).info(format, arguments.toArray)
  def info[T](caller: T, message: String, exception: Throwable) = logger(caller).info(message, exception)
  def info[T](caller: T, marker: Marker, message: String) = logger(caller).info(marker, message)
  def info[T](caller: T, marker: Marker, format: String, arguments: Object*) = logger(caller).info(marker, format, arguments.toArray)
  def info[T](caller: T, marker: Marker, message: String, exception: Throwable) = logger(caller).info(marker, message, exception)
  def isInfoEnabled[T](caller: T) = logger(caller).isInfoEnabled
  def isInfoEnabled[T](caller: T, marker: Marker) = logger(caller).isInfoEnabled(marker)

  def trace[T](caller: T, message: String) = logger(caller).trace(message)
  def trace[T](caller: T, format: String, arguments: Object*) = logger(caller).trace(format, arguments.toArray)
  def trace[T](caller: T, message: String, exception: Throwable) = logger(caller).trace(message, exception)
  def trace[T](caller: T, marker: Marker, message: String) = logger(caller).trace(marker, message)
  def trace[T](caller: T, marker: Marker, format: String, arguments: Object*) = logger(caller).trace(marker, format, arguments.toArray)
  def trace[T](caller: T, marker: Marker, message: String, exception: Throwable) = logger(caller).trace(marker, message, exception)
  def isTraceEnabled[T](caller: T) = logger(caller).isTraceEnabled
  def isTraceEnabled[T](caller: T, marker: Marker) = logger(caller).isTraceEnabled(marker)

  def warn[T](caller: T, message: String) = logger(caller).warn(message)
  def warn[T](caller: T, format: String, arguments: Object*) = logger(caller).warn(format, arguments.toArray)
  def warn[T](caller: T, message: String, exception: Throwable) = logger(caller).warn(message, exception)
  def warn[T](caller: T, marker: Marker, message: String) = logger(caller).warn(marker, message)
  def warn[T](caller: T, marker: Marker, format: String, arguments: Object*) = logger(caller).warn(marker, format, arguments.toArray)
  def warn[T](caller: T, marker: Marker, message: String, exception: Throwable) = logger(caller).warn(marker, message, exception)
  def isWarnEnabled[T](caller: T) = logger(caller).isWarnEnabled
  def isWarnEnabled[T](caller: T, marker: Marker) = logger(caller).isWarnEnabled(marker)
}

