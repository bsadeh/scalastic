package org.slf4j

object Log {
  def logger[T](implicit m: ClassManifest[T]) = LoggerFactory.getLogger(m.erasure)

  def debug[T](message: String)(implicit m: ClassManifest[T]) = logger.debug(message)
  def debug[T](message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.debug(message, subject)
  def debug[T](message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.debug(message, subject, subject2)
  def debug[T](message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.debug(message, subjects.toArray)
  def debug[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.debug(message, exception)
  def debug[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.debug(marker, message)
  def debug[T](marker: Marker, message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.debug(marker, message, subject)
  def debug[T](marker: Marker, message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.debug(marker, message, subject, subject2)
  def debug[T](marker: Marker, message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.debug(marker, message, subjects.toArray)
  def debug[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.debug(marker, message, exception)
  def isDebugEnabled[T](implicit m: ClassManifest[T]) = logger.isDebugEnabled
  def isDebugEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isDebugEnabled(marker)
  
  def error[T](message: String)(implicit m: ClassManifest[T]) = logger.error(message)
  def error[T](message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.error(message, subject)
  def error[T](message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.error(message, subject, subject2)
  def error[T](message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.error(message, subjects.toArray)
  def error[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.error(message, exception)
  def error[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.error(marker, message)
  def error[T](marker: Marker, message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.error(marker, message, subject)
  def error[T](marker: Marker, message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.error(marker, message, subject, subject2)
  def error[T](marker: Marker, message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.error(marker, message, subjects.toArray)
  def error[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.error(marker, message, exception)
  def isErrorEnabled[T](implicit m: ClassManifest[T]) = logger.isErrorEnabled
  def isErrorEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isErrorEnabled(marker)
  
  def info[T](message: String)(implicit m: ClassManifest[T]) = logger.info(message)
  def info[T](message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.info(message, subject)
  def info[T](message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.info(message, subject, subject2)
  def info[T](message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.info(message, subjects.toArray)
  def info[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.info(message, exception)
  def info[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.info(marker, message)
  def info[T](marker: Marker, message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.info(marker, message, subject)
  def info[T](marker: Marker, message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.info(marker, message, subject, subject2)
  def info[T](marker: Marker, message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.info(marker, message, subjects.toArray)
  def info[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.info(marker, message, exception)
  def isInfoEnabled[T](implicit m: ClassManifest[T]) = logger.isInfoEnabled
  def isInfoEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isInfoEnabled(marker)
  
  def trace[T](message: String)(implicit m: ClassManifest[T]) = logger.trace(message)
  def trace[T](message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.trace(message, subject)
  def trace[T](message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.trace(message, subject, subject2)
  def trace[T](message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.trace(message, subjects.toArray)
  def trace[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.trace(message, exception)
  def trace[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.trace(marker, message)
  def trace[T](marker: Marker, message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.trace(marker, message, subject)
  def trace[T](marker: Marker, message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.trace(marker, message, subject, subject2)
  def trace[T](marker: Marker, message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.trace(marker, message, subjects.toArray)
  def trace[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.trace(marker, message, exception)
  def isTraceEnabled[T](implicit m: ClassManifest[T]) = logger.isTraceEnabled
  def isTraceEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isTraceEnabled(marker)
  
  def warn[T](message: String)(implicit m: ClassManifest[T]) = logger.warn(message)
  def warn[T](message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.warn(message, subject)
  def warn[T](message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.warn(message, subject, subject2)
  def warn[T](message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.warn(message, subjects.toArray)
  def warn[T](message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.warn(message, exception)
  def warn[T](marker: Marker, message: String)(implicit m: ClassManifest[T]) = logger.warn(marker, message)
  def warn[T](marker: Marker, message: String, subject: Object)(implicit m: ClassManifest[T]) = logger.warn(marker, message, subject)
  def warn[T](marker: Marker, message: String, subject: Object, subject2: Object)(implicit m: ClassManifest[T]) = logger.warn(marker, message, subject, subject2)
  def warn[T](marker: Marker, message: String, subjects: Seq[Object])(implicit m: ClassManifest[T]) = logger.warn(marker, message, subjects.toArray)
  def warn[T](marker: Marker, message: String, exception: Throwable)(implicit m: ClassManifest[T]) = logger.warn(marker, message, exception)
  def isWarnEnabled[T](implicit m: ClassManifest[T]) = logger.isWarnEnabled
  def isWarnEnabled[T](marker: Marker)(implicit m: ClassManifest[T]) = logger.isWarnEnabled(marker)
}

