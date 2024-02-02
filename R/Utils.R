loadRenderTranslateSql <- function(
  sqlFilename, 
  packageName, 
  dbms = "sql server", 
  ..., 
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"), 
  warnOnMissingParameters = TRUE) {

  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(sqlFilename, len = 1, add = errorMessages)
  checkmate::assertCharacter(packageName, len = 1, add = errorMessages)
  checkmate::assertCharacter(dbms, len = 1, add = errorMessages)
  checkmate::assertChoice(dbms, SqlRender::listSupportedDialects()$dialect, add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertLogical(warnOnMissingParameters, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  pathToSql <- system.file("sql", gsub(" ", "_", dbms), sqlFilename, package = packageName)
  
  mustTranslate <- pathToSql == ""
  
  if (mustTranslate) {
    pathToSql <- system.file("sql", "sql_server", sqlFilename, package = packageName)
    if (pathToSql == "") {
      pathToSql <- system.file("sql", sqlFilename, package = packageName)
    }
    
    if (pathToSql == "") {
      cli::cli_abort("Cannot find '{sqlFilename}' in the 'sql' or 'sql/sql_server' folder of the '{packageName}' package.")
    }
  }
  
  parameterizedSql <- readChar(pathToSql, file.info(pathToSql)$size)
  
  renderedSql <- SqlRender::render(sql = parameterizedSql[1], 
                                   warnOnMissingParameters = warnOnMissingParameters, 
                                   ...)
  
  if (mustTranslate) {
    renderedSql <- SqlRender::translate(sql = renderedSql, targetDialect = dbms, tempEmulationSchema = tempEmulationSchema)
  }
  
  renderedSql
}
  
