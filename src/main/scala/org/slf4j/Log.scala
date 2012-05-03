package org.slf4j

object Log {
  def logger[T](source: T) = LoggerFactory.getLogger(source.getClass)
  def logger[T](`class`: Class[T]) = LoggerFactory.getLogger(`class`)
  def logger(name: String) = LoggerFactory.getLogger(name)

  def debug[T](source: T, message: String) = logger(source).debug(message)
  def debug[T](source: T, format: String, arguments: Object*) = logger(source).debug(format, arguments.toArray)
  def debug[T](source: T, message: String, exception: Throwable) = logger(source).debug(message, exception)
  def debug[T](source: T, marker: Marker, message: String) = logger(source).debug(marker, message)
  def debug[T](source: T, marker: Marker, format: String, arguments: Object*) = logger(source).debug(marker, format, arguments.toArray)
  def debug[T](source: T, marker: Marker, message: String, exception: Throwable) = logger(source).debug(marker, message, exception)
  def isDebugEnabled[T](source: T) = logger(source).isDebugEnabled
  def isDebugEnabled[T](source: T, marker: Marker) = logger(source).isDebugEnabled(marker)

  def error[T](source: T, message: String) = logger(source).error(message)
  def error[T](source: T, format: String, arguments: Object*) = logger(source).error(format, arguments.toArray)
  def error[T](source: T, message: String, exception: Throwable) = logger(source).error(message, exception)
  def error[T](source: T, marker: Marker, message: String) = logger(source).error(marker, message)
  def error[T](source: T, marker: Marker, format: String, arguments: Object*) = logger(source).error(marker, format, arguments.toArray)
  def error[T](source: T, marker: Marker, message: String, exception: Throwable) = logger(source).error(marker, message, exception)
  def isErrorEnabled[T](source: T) = logger(source).isErrorEnabled
  def isErrorEnabled[T](source: T, marker: Marker) = logger(source).isErrorEnabled(marker)

  def info[T](source: T, message: String) = logger(source).info(message)
  def info[T](source: T, format: String, arguments: Object*) = logger(source).info(format, arguments.toArray)
  def info[T](source: T, message: String, exception: Throwable) = logger(source).info(message, exception)
  def info[T](source: T, marker: Marker, message: String) = logger(source).info(marker, message)
  def info[T](source: T, marker: Marker, format: String, arguments: Object*) = logger(source).info(marker, format, arguments.toArray)
  def info[T](source: T, marker: Marker, message: String, exception: Throwable) = logger(source).info(marker, message, exception)
  def isInfoEnabled[T](source: T) = logger(source).isInfoEnabled
  def isInfoEnabled[T](source: T, marker: Marker) = logger(source).isInfoEnabled(marker)

  def trace[T](source: T, message: String) = logger(source).trace(message)
  def trace[T](source: T, format: String, arguments: Object*) = logger(source).trace(format, arguments.toArray)
  def trace[T](source: T, message: String, exception: Throwable) = logger(source).trace(message, exception)
  def trace[T](source: T, marker: Marker, message: String) = logger(source).trace(marker, message)
  def trace[T](source: T, marker: Marker, format: String, arguments: Object*) = logger(source).trace(marker, format, arguments.toArray)
  def trace[T](source: T, marker: Marker, message: String, exception: Throwable) = logger(source).trace(marker, message, exception)
  def isTraceEnabled[T](source: T) = logger(source).isTraceEnabled
  def isTraceEnabled[T](source: T, marker: Marker) = logger(source).isTraceEnabled(marker)

  def warn[T](source: T, message: String) = logger(source).warn(message)
  def warn[T](source: T, format: String, arguments: Object*) = logger(source).warn(format, arguments.toArray)
  def warn[T](source: T, message: String, exception: Throwable) = logger(source).warn(message, exception)
  def warn[T](source: T, marker: Marker, message: String) = logger(source).warn(marker, message)
  def warn[T](source: T, marker: Marker, format: String, arguments: Object*) = logger(source).warn(marker, format, arguments.toArray)
  def warn[T](source: T, marker: Marker, message: String, exception: Throwable) = logger(source).warn(marker, message, exception)
  def isWarnEnabled[T](source: T) = logger(source).isWarnEnabled
  def isWarnEnabled[T](source: T, marker: Marker) = logger(source).isWarnEnabled(marker)
}

