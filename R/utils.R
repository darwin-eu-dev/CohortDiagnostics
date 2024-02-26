
# internal function
loadRenderTranslateSql <- function(
    sqlFilename, 
    packageName, 
    dbms = "sql server", 
    ..., 
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), 
    oracleTempSchema = NULL, 
    warnOnMissingParameters = TRUE) 
  {
  
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(sqlFilename, len = 1, add = errorMessages)
  checkmate::assertCharacter(packageName, len = 1, add = errorMessages)
  checkmate::assertCharacter(dbms, len = 1, add = errorMessages)
  checkmate::assertChoice(dbms, SqlRender::listSupportedDialects()$dialect, add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(oracleTempSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertLogical(warnOnMissingParameters, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    rlang::warn("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.", 
                .frequency = "regularly", .frequency_id = "oracleTempSchema")
    tempEmulationSchema <- oracleTempSchema
  }
  pathToSql <- system.file("sql", gsub(" ", "_", dbms), sqlFilename, package = "CohortDiagnostics")
  mustTranslate <- pathToSql == ""
  if (mustTranslate) {
    pathToSql <- system.file("sql", "sql_server", sqlFilename, package = packageName)
    if (pathToSql == "") {
      pathToSql <- system.file("sql", sqlFilename, package = packageName)
    }
    if (pathToSql == "") {
      abort(sprintf("Cannot find '%s' in the 'sql' or 'sql/sql_server' folder of the '%s' package.", 
                    sqlFilename, packageName))
    }
  }
  parameterizedSql <- readChar(pathToSql, file.info(pathToSql)$size)
  renderedSql <- SqlRender::render(sql = parameterizedSql[1], warnOnMissingParameters = warnOnMissingParameters, 
                        ...)
  if (mustTranslate) {
    renderedSql <- SqlRender::translate(sql = renderedSql, targetDialect = dbms, 
                             tempEmulationSchema = tempEmulationSchema)
  }
  renderedSql
}