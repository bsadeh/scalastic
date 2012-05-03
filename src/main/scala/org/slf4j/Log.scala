package org.slf4j

object Log {
  def logger[T](implicit manifest: ClassManifest[T]) = LoggerFactory.getLogger(manifest.erasure)

  def debug[T](message: String)(implicit m: ClassManifest[T]) = logger.debug(message)
  def debug[T](format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.debug(format, arguments.toArray)
  def debug[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.debug(message, exception)
  def debug[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.debug(marker, message)
  def debug[T](marker: Marker, format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.debug(marker, format, arguments.toArray)
  def debug[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.debug(marker, message, exception)
  def isDebugEnabled[T](implicit m: ClassManifest[T]) = logger.isDebugEnabled
  def isDebugEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isDebugEnabled(marker)

  def error[T](message: String)(implicit m: ClassManifest[T]) = logger.error(message)
  def error[T](format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.error(format, arguments.toArray)
  def error[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.error(message, exception)
  def error[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.error(marker, message)
  def error[T](marker: Marker, format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.error(marker, format, arguments.toArray)
  def error[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.error(marker, message, exception)
  def isErrorEnabled[T](implicit m: ClassManifest[T]) = logger.isErrorEnabled
  def isErrorEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isErrorEnabled(marker)

  def info[T](message: String)(implicit m: ClassManifest[T]) = logger.info(message)
  def info[T](format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.info(format, arguments.toArray)
  def info[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.info(message, exception)
  def info[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.info(marker, message)
  def info[T](marker: Marker, format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.info(marker, format, arguments.toArray)
  def info[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.info(marker, message, exception)
  def isInfoEnabled[T](implicit m: ClassManifest[T]) = logger.isInfoEnabled
  def isInfoEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isInfoEnabled(marker)

  def trace[T](message: String)(implicit m: ClassManifest[T]) = logger.trace(message)
  def trace[T](format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.trace(format, arguments.toArray)
  def trace[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.trace(message, exception)
  def trace[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.trace(marker, message)
  def trace[T](marker: Marker, format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.trace(marker, format, arguments.toArray)
  def trace[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.trace(marker, message, exception)
  def isTraceEnabled[T](implicit m: ClassManifest[T]) = logger.isTraceEnabled
  def isTraceEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isTraceEnabled(marker)

  def warn[T](message: String)(implicit m: ClassManifest[T]) = logger.warn(message)
  def warn[T](format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.warn(format, arguments.toArray)
  def warn[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.warn(message, exception)
  def warn[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.warn(marker, message)
  def warn[T](marker: Marker, format: String, arguments: Object*)(implicit m: ClassManifest[T]) = logger.warn(marker, format, arguments.toArray)
  def warn[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.warn(marker, message, exception)
  def isWarnEnabled[T](implicit m: ClassManifest[T]) = logger.isWarnEnabled
  def isWarnEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isWarnEnabled(marker)
}

