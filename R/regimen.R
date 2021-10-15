
#' Create Oncology Regimens
#'
#' @param connectionDetails connectionDetails to an OMOP CDM created by DatabaseConnector::createConnectionDetails()
#' @param cdmDatabaseSchema (character) Schema that contains the OMOP CDM
#' @param writeDatabaseSchema (character) Schema where the user has write access
#' @param cohortTable (character) The name of the cohort table that will be created in the writeDatabaseSchema
#' @param regimenTable (character) Name of the regimen table that will be created in the writeDatabaseSchema
#' @param regimenIngredientTable Name of regimenIngredientTable
#' @param vocabularyTable 
#' @param drug_classification_id_input 
#' @param date_lag_input 
#' @param sample_size 
#' @param regimen_repeats 
#' @param generateVocabTable 
#'
#' @return
#' @importFrom magrittr `%>%`
#' @export
#'
#' @examples
create_regimens <- function(connectionDetails, 
                            cdmDatabaseSchema, 
                            writeDatabaseSchema, 
                            cohortTable = "cohort", 
                            regimenTable = "regimen", 
                            regimenIngredientTable = "regimenIngredient", 
                            vocabularyTable = vocabularyTable, 
                            drug_classification_id_input, 
                            date_lag_input, 
                            sample_size = 1e9, 
                            regimen_repeats = 5, 
                            generateVocabTable = FALSE){
  
  stopifnot(methods::is(connectionDetails, "connectionDetails"))
  
  # Build cohort table and regimen table which contains drug eras for drug_classification_id_input classification code
  sql <- SqlRender::readSql(system.file("buildRegimenCohorts.sql", "Care")) %>% 
    SqlRender::render(cdmDatabaseSchema = cdmDatabaseSchema, 
                      writeDatabaseSchema = writeDatabaseSchema, 
                      cohortTable = cohortTable, 
                      regimenTable = regimenTable, 
                      drug_classification_id_input = drug_classification_id_input) %>% 
    SqlRender::translate(targetDialect = connectionDetails$dbms)
  
  connection <-  DatabaseConnector::connect(connectionDetails)
  DatabaseConnector::executeSql(connection, sql)
  
  sql <- SqlRender::render("SELECT max(rownum) as max FROM @writeDatabaseSchema.@regimenTable;", 
                           writeDatabaseSchema = writeDatabaseSchema, 
                           regimenTable = regimenTable)
  
  max_id <- DatabaseConnector::dbGetQuery(connection, sql)
  
  # Alternate implementation
  # Why not count distinct person_ids?
  num_persons <- glue::glue("SELECT COUNT(DISTINCT person_id) AS max FROM {writeDatabaseSchema}.{regimenTable};") %>% 
    SqlRender::translate(targetDialect = connectionDetails$dbms) %>% 
    DatabaseConnector::dbGetQuery(connection, .) %>% 
    dplyr::pull(max)
  
  message(paste0("Cohort contains ", max_id, " subjects"))
  
  # Why are we counting by sample_size?
  id_groups <- c(seq(1, num_persons, sample_size), max_id + 1)
  
  sql <- SqlRender::render("
    DROP TABLE IF EXISTS @writeDatabaseSchema.@regimenTable_f;
    CREATE TABLE @writeDatabaseSchema.@regimenTable_f (
       person_id bigint not null,
       drug_era_id bigint,
       concept_name varchar(max),
       ingredient_start_date date not null
    ) DISTKEY(person_id) SORTKEY(person_id, ingredient_start_date);", 
                           regimenTable_f = paste0(regimenTable,"_f"), 
                           writeDatabaseSchema = writeDatabaseSchema)
  
  DatabaseConnector::executeSql(connection, sql)
  
  for(g in c(1:(length(id_groups)-1))){
    
    start_id = id_groups[g]
    end_id = id_groups[g+1] - 1
    
    message(paste0("Processing persons ",start_id," to ",end_id))
    
    sql <- SqlRender::render("DROP TABLE IF EXISTS @writeDatabaseSchema.@sampledRegimenTable;
                              SELECT person_id, drug_era_id, concept_name, ingredient_start_date
                              into @writeDatabaseSchema.@sampledRegimenTable
                              FROM @writeDatabaseSchema.@regimenTable
                              WHERE rn >= @start AND rn <= @end;",
                             writeDatabaseSchema = writeDatabaseSchema, 
                             regimenTable = regimenTable, 
                             sampledRegimenTable = paste0(regimenTable,"_sampled"), 
                             start = start_id, end = end_id)
    
    DatabaseConnector::executeSql(connection, sql)
    
    sql <- SqlRender::loadRenderTranslateSql("RegimenCalc2.sql", 
                                             packageName = "OncoRegimenFinder", 
                                             dbms = "redshift",
                                             writeDatabaseSchema = writeDatabaseSchema, 
                                             regimenTable = paste0(regimenTable,"_sampled"), 
                                             date_lag_input = date_lag_input)
    # sql <- SqlRender::translate(sql,targetDialect = connectionDetails$dbms)
    
    for(i in c(1:regimen_repeats)){DatabaseConnector::executeSql(connection, sql)}
    
    sql <- SqlRender::render("insert into @writeDatabaseSchema.@regimenTable_f
                              (select *
                              from @writeDatabaseSchema.@sampledRegimenTable);",  writeDatabaseSchema = writeDatabaseSchema, sampledRegimenTable = paste0(regimenTable,"_sampled"), regimenTable_f = paste0(regimenTable,"_f"))
    
    DatabaseConnector::executeSql(connection, sql)
    
  }
  
  
  if(generateVocabTable){
    
    
    sql <- SqlRender::loadRenderTranslateSql("RegimenVoc.sql", packageName = "OncoRegimenFinder", dbms = "redshift", cdmDatabaseSchema = cdmDatabaseSchema, writeDatabaseSchema = writeDatabaseSchema, vocabularyTable = vocabularyTable)
    sql <- SqlRender::translate(sql,targetDialect = connectionDetails$dbms)
    DatabaseConnector::executeSql(connection, sql)
    
  }
  
  sql <- SqlRender::loadRenderTranslateSql("RegimenFormat.sql", packageName = "OncoRegimenFinder", dbms = "redshift", writeDatabaseSchema = writeDatabaseSchema, cohortTable = cohortTable, regimenTable = paste0(regimenTable,"_f"), regimenIngredientTable = regimenIngredientTable, vocabularyTable = vocabularyTable)
  
  DatabaseConnector::executeSql(connection, sql)
  
  
}