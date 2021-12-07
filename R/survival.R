#' @export
generateSurvival <- function(connection,
                             cohortDatabaseSchema,
                             cohortTable,
                             targetIds,
                             outcomeId,
                             packageName,
                             databaseId) {
  
  sql <- "
  SELECT t.subject_id,
  	 t.cohort_start_date,
  	 coalesce(min(o.cohort_start_date), max(t.cohort_end_date)) AS event_date,
  	 CASE WHEN min(o.cohort_start_date) IS NULL THEN 0 ELSE 1 END AS event
  FROM @cohort_database_schema.@cohort_table t
  LEFT JOIN @cohort_database_schema.@cohort_table o
    ON t.subject_id = o.subject_id
  	  AND o.cohort_start_date >= t.cohort_start_date
  	  AND o.cohort_start_date <= t.cohort_end_date
  	  AND o.cohort_definition_id = @outcome_id
  WHERE t.cohort_definition_id = @target_id
  GROUP BY t.subject_id, t.cohort_start_date;

  "
  
  survOutputs <- purrr::map_dfr(targetIds, function(targetId) {
    sqlTmp <- SqlRender::render(sql,
                                cohort_database_schema = cohortDatabaseSchema,
                                cohort_table = cohortTable,
                                outcome_id = outcomeId,
                                target_id = targetId
    )
    sqlTmp <- SqlRender::translate(
      sql = sqlTmp,
      targetDialect = connection@dbms
    )
    
    kmRaw <- DatabaseConnector::querySql(
      connection = connection,
      sql = sqlTmp,
      snakeCaseToCamelCase = T
    )
    
    ## edit
    if (nrow(kmRaw) < 100 | length(kmRaw$event[kmRaw$event == 1]) < 1) {
      return(NULL)
    }
    
    km_proc <- kmRaw %>%
      dplyr::mutate(
        timeToEvent = as.integer(as.Date(eventDate) - as.Date(cohortStartDate)),
        id = dplyr::row_number()
      ) %>%
      dplyr::select(id, timeToEvent, event)
    
    survInfo <- survival::survfit(survival::Surv(timeToEvent, event) ~ 1, data = km_proc)
    
    survInfo <- survminer::surv_summary(survInfo)
    
    data.frame(
      targetId = targetId,
      outcomeId = outcomeId,
      time = survInfo$time,
      surv = survInfo$surv,
      n.censor = survInfo$n.censor,
      n.event = survInfo$n.event,
      n.risk = survInfo$n.risk,
      lower = survInfo$lower,
      upper = survInfo$upper,
      databaseId = databaseId
    )
  })
}
