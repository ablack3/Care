#' Identify drug regimen exposures
#'
#' This function will create a new table in an OMOP CDM database that contains drug regimens exposures for each person.
#' Regimens are defined continuous periods of time where one or more ingredients taken within 30 days of each other.
#'
#' @details
#' This algorithm is based largely on OHDSI's drug era logic (https://ohdsi.github.io/CommonDataModel/sqlScripts.html#Drug_Eras).
#' The major difference is that instead of creating a different era for each ingredient the regimen finder creates eras for combinations
#' of ingredients and matches them to user specified regimens (i.e. ingredient combinations).
#'
#' Ingredients that are not part of any regimen are completely ignored by the algorithm.
#' The first step is to roll up drug exposures to the RxNorm ingredient level.
#' Then considering only ingredients that are part of at least one regimen in the user's input the algorithm
#' creates exposure eras with 30 day collapsing logic that ignore ingredient. These eras are continuous periods of exposure to any ingredient in at least one regimen.
#' Next the algorithm identifies all ingredients exposures that occur within an each exposure era.
#' If the complete set of ingredients in an era matches the set of ingredients in a regimen definition then we have identified a regimen exposure
#' and a new record will be created in the final regimen table.
#'
#' This function should work on any suppported OHDSI database platform.
#'
#' @param con A DatabaseConnectorJdbcConnection object
#' @param regimenIngredient A dataframe that contains the regimen definitions
#' @param cdmDatabaseSchema The schema containing an OMOP CDM in the database
#' @param writeDatabaseSchema The name of the schema where the results should be saved in the database. Write access is required. If NULL (default) then result will be written to a temp table.
#' @param regimenTableName The name of the results table that will contain the regimens
#'
#' @return Returns NULL. This function is called for its side effect of creating a regimen table in the CDM database
#' @export
#'
#' @examples
#'
#' library(Eunomia)
#' # create or derive a dataframe that defines regimens
#' regimenIngredient <- data.frame(regimen_name = c("Venetoclax and Obinutuzumab", "Venetoclax and Obinutuzumab", "Doxycycline monotherapy"),
#'                                 regimen_id = c(35100084L, 35100084L, 35806103),
#'                                 ingredient_name = c("venetoclax", "obinutuzumab", "Doxycycline"),
#'                                 ingredient_concept_id = c(35604205L, 44507676L, 1738521))
#'
#' cd <- getEunomiaConnectionDetails()
#' con <- connect(cd)
#' createRegimens(con, regimenIngredient, "main", "main", "myregimens")
#'
#' # download the result from the database
#' regimens <- dbGetQuery(con, "select * from myregimens")
#'
createRegimens <- function(con, regimenIngredient, cdmDatabaseSchema, writeDatabaseSchema = NULL, regimenTableName = "regimen") {
  
  # verify input
  stopifnot(is.data.frame(regimenIngredient), names(regimenIngredient) == c("regimen_name", "regimen_id", "ingredient_name", "ingredient_concept_id"))
  
  if(con@dbms %in% c("bigquery", "oracle") & Sys.getenv("sqlRenderTempEmulationSchema") == "") {
    rlang::abort("sqlRenderTempEmulationSchema environment variable must be set when using bigquery or oracle.")
  }
  rlang::inform("Loading regimenIngredient into the database.")
  DatabaseConnector::insertTable(con,
                                 tableName = "regimenIngredient",
                                 data = regimenIngredient,
                                 tempTable = TRUE,
                                 dropTableIfExists = TRUE,
                                 tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema"),
                                 progressBar = TRUE)
  check <- dbGetQuery(con, SqlRender::translate("SELECT * FROM #regimenIngredient", con@dbms, tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema")))
  if(nrow(regimenIngredient) != nrow(check)) rlang::abort("regimenIngredient was not uploaded to the database.")
  
  sql <- "
  /****
  DRUG ERA
  Note: Eras derived from DRUG_EXPOSURE table, using 30d gap.
  Era collapsing logic copied and modified from https://ohdsi.github.io/CommonDataModel/sqlScripts.html#Drug_Eras
   ****/
  DROP TABLE IF EXISTS #cteDrugTarget;

  /* / */

  -- Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date, or add days supply, or add 1 day to the start date
  SELECT d.DRUG_EXPOSURE_ID
      ,d.PERSON_ID
      ,c.CONCEPT_ID AS INGREDIENT_CONCEPT_ID
      ,d.DRUG_TYPE_CONCEPT_ID
      ,DRUG_EXPOSURE_START_DATE
      ,COALESCE(DRUG_EXPOSURE_END_DATE, DATEADD(day, DAYS_SUPPLY, DRUG_EXPOSURE_START_DATE), DATEADD(day, 1, DRUG_EXPOSURE_START_DATE)) AS DRUG_EXPOSURE_END_DATE
  INTO #cteDrugTarget
  FROM @TARGET_CDMV5_SCHEMA.DRUG_EXPOSURE d
  INNER JOIN @TARGET_CDMV5_SCHEMA.CONCEPT_ANCESTOR ca ON ca.DESCENDANT_CONCEPT_ID = d.DRUG_CONCEPT_ID
  INNER JOIN @TARGET_CDMV5_SCHEMA.CONCEPT c ON ca.ANCESTOR_CONCEPT_ID = c.CONCEPT_ID
  WHERE c.DOMAIN_ID = 'Drug'
      AND c.CONCEPT_CLASS_ID = 'Ingredient'
      AND c.CONCEPT_ID IN(@ingredient_ids);

  /* / */

  DROP TABLE IF EXISTS #cteEndDates;

  /* / */

  SELECT PERSON_ID
      ,DATEADD(day, - 30, EVENT_DATE) AS END_DATE -- unpad the end date
  INTO #cteEndDates
  FROM (
      SELECT E1.PERSON_ID
          ,E1.EVENT_DATE
          ,COALESCE(E1.START_ORDINAL, MAX(E2.START_ORDINAL)) START_ORDINAL
          ,E1.OVERALL_ORD
      FROM (
          SELECT PERSON_ID
              ,EVENT_DATE
              ,EVENT_TYPE
              ,START_ORDINAL
              ,ROW_NUMBER() OVER (
                  PARTITION BY PERSON_ID ORDER BY EVENT_DATE, EVENT_TYPE
                  ) AS OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
          FROM (
              -- select the start dates, assigning a row number to each
              SELECT PERSON_ID
                  ,DRUG_EXPOSURE_START_DATE AS EVENT_DATE
                  ,0 AS EVENT_TYPE
                  ,ROW_NUMBER() OVER (
                      PARTITION BY PERSON_ID ORDER BY DRUG_EXPOSURE_START_DATE
                      ) AS START_ORDINAL
              FROM #cteDrugTarget

              UNION ALL

              -- add the end dates with NULL as the row number, padding the end dates by 30 to allow a grace period for overlapping ranges.
              SELECT PERSON_ID
                  ,DATEADD(day, 30, DRUG_EXPOSURE_END_DATE)
                  ,1 AS EVENT_TYPE
                  ,NULL
              FROM #cteDrugTarget
              ) RAWDATA
          ) E1
      INNER JOIN (
          SELECT PERSON_ID
              ,DRUG_EXPOSURE_START_DATE AS EVENT_DATE
              ,ROW_NUMBER() OVER (
                  PARTITION BY PERSON_ID ORDER BY DRUG_EXPOSURE_START_DATE
                  ) AS START_ORDINAL
          FROM #cteDrugTarget
          ) E2 ON E1.PERSON_ID = E2.PERSON_ID
          AND E2.EVENT_DATE <= E1.EVENT_DATE
      GROUP BY E1.PERSON_ID
          ,E1.EVENT_DATE
          ,E1.START_ORDINAL
          ,E1.OVERALL_ORD
      ) E
  WHERE 2 * E.START_ORDINAL - E.OVERALL_ORD = 0;

  /* / */

  DROP TABLE IF EXISTS #cteDrugExpEnds;

  /* / */

  SELECT d.PERSON_ID
      ,d.DRUG_TYPE_CONCEPT_ID
      ,d.DRUG_EXPOSURE_START_DATE
      ,MIN(e.END_DATE) AS ERA_END_DATE
  INTO #cteDrugExpEnds
  FROM #cteDrugTarget d
  INNER JOIN #cteEndDates e ON d.PERSON_ID = e.PERSON_ID
      AND e.END_DATE >= d.DRUG_EXPOSURE_START_DATE
  GROUP BY d.PERSON_ID
      ,d.DRUG_TYPE_CONCEPT_ID
      ,d.DRUG_EXPOSURE_START_DATE;

  /* / */

  DROP TABLE IF EXISTS #exposureEra;

  SELECT
    row_number() OVER (ORDER BY person_id) AS drug_era_id
    ,person_id
    ,era_start_date
    ,era_end_date
  INTO #exposureEra
  FROM (
    SELECT
    person_id
    ,min(DRUG_EXPOSURE_START_DATE) AS era_start_date
    ,ERA_END_DATE as era_end_date
    FROM #cteDrugExpEnds
    GROUP BY person_id
      ,drug_type_concept_id
      ,ERA_END_DATE
  );

  -- Add ingredients to eras
  DROP TABLE IF EXISTS #comboIngredientEras;

  SELECT DISTINCT
    e.drug_era_id
    ,e.person_id as person_id
    ,e.era_start_date
    ,e.era_end_date
    ,i.INGREDIENT_CONCEPT_ID AS ingredient_concept_id
    ,COUNT(i.ingredient_concept_id) OVER(PARTITION BY e.drug_era_id) AS num_ingredients_in_era
  INTO #comboIngredientEras
  FROM
  #exposureEra e
  LEFT JOIN #cteDrugTarget i
    ON e.person_id = i.PERSON_ID
    AND i.DRUG_EXPOSURE_START_DATE >= e.era_start_date
    AND i.DRUG_EXPOSURE_START_DATE <= e.era_end_date;

  -- Match comination ingredient eras with regimens
  -- If an exposure era has the same ingredients as a regimen we have a match
  DROP TABLE IF EXISTS #regimenEraMatch;

  -- save the matches
  SELECT DISTINCT
    drug_era_id
    ,regimen_id
    ,regimen_name
  INTO #regimenEraMatch
  FROM (
  -- next count up the number of ingredients that are in both the regimen and era
  SELECT 
    drug_era_id
    ,regimen_id
    ,regimen_name
    ,MAX(num_ingredients_in_regimen) as num_ingredients_in_regimen
    ,MAX(num_ingredients_in_era) as num_ingredients_in_era
    ,COUNT(1) as num_ingredients_matched
  FROM (
    -- first inner join the eras with regimens on ingredient_id
    SELECT DISTINCT
      e.drug_era_id
      ,e.ingredient_concept_id
      ,e.num_ingredients_in_era
      ,r.regimen_id
      ,r.regimen_name
      ,r.num_ingredients_in_regimen
    FROM #comboIngredientEras e
    INNER JOIN (
      SELECT *, COUNT(ingredient_concept_id) OVER(PARTITION BY regimen_id) AS num_ingredients_in_regimen from #regimenIngredient
    ) r
    ON e.ingredient_concept_id = r.ingredient_concept_id
  ) cte
  GROUP BY drug_era_id, regimen_id, regimen_name
  )
  WHERE num_ingredients_in_regimen = num_ingredients_in_era 
    AND num_ingredients_in_era = num_ingredients_matched;

  DROP TABLE IF EXISTS #@regimenTableName;
  
  SELECT DISTINCT
    m.drug_era_id 
    ,e.person_id
    ,e.era_start_date AS regimen_start_date
    ,e.era_end_date AS regimen_end_date
    ,m.regimen_id
    ,m.regimen_name
  INTO #@regimenTableName
  FROM #regimenEraMatch m
  LEFT JOIN #comboIngredientEras e
    ON m.drug_era_id = e.drug_era_id;
  "
  rlang::inform("Calculating regimens.")
  DatabaseConnector::renderTranslateExecuteSql(con,
                                               sql,
                                               TARGET_CDMV5_SCHEMA = cdmDatabaseSchema,
                                               ingredient_ids = regimenIngredient$ingredient_concept_id,
                                               regimenTableName = regimenTableName,
                                               tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema"))
  
  sql <- SqlRender::render("SELECT COUNT(*) as n FROM #@regimenTableName", regimenTableName = regimenTableName)
  sql <- SqlRender::translate(sql, con@dbms, tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema"))
  n <- DatabaseConnector::dbGetQuery(con, sql)$n
  if(n == 0) warning("0 regimens found")
  
  if(!is.null(writeDatabaseSchema)) {
    sql <- "
    DROP TABLE IF EXISTS @writeDatabaseSchema.@regimenTableName;

    SELECT *
    INTO @writeDatabaseSchema.@regimenTableName
    FROM #@regimenTableName;"
    tryCatch(
      DatabaseConnector::renderTranslateExecuteSql(con, sql, regimenTableName = regimenTableName, writeDatabaseSchema = writeDatabaseSchema, tempEmulationSchema = Sys.getenv("sqlRenderTempEmulationSchema")),
      error = function(e) {
        message(paste0("\nRegimen table with ", n, " rows saved as temporary table named ", regimenTableName))
        warning(paste0("\nWriting regimen table to ", writeDatabaseSchema, ".", regimenTableName, " failed"))
        warning(e)
      })
    # might check that the schema exists first and user has write access
    message(paste0("\nRegimen table with ", n, " rows saved to ", writeDatabaseSchema, ".", regimenTableName))
  } else {
    message(paste0("\nRegimen table with ", n, " rows saved as temporary table named ", regimenTableName))
  }
  invisible(NULL)
}




#' Create regimen stats table
#' @return
#' The function returns nothing. As side effect it creates regimen stats table in write database schema
#'
#' @export
createRegimenStats <- function(connectionDetails,
                               cdmDatabaseSchema,
                               writeDatabaseSchema,
                               cohortTable,
                               regimenStatsTable,
                               regimenIngredientsTable,
                               gapBetweenTreatment = 120) {
  
  cohortDatabaseSchema <- writeDatabaseSchema
  
  sql <- "
  DROP TABLE IF
  EXISTS @cohortDatabaseSchema.@regimenStatsTable;
  
  CREATE table @cohortDatabaseSchema.@regimenStatsTable (
               cohort_definition_id int,
               person_id bigint,
               Line_of_therapy int,
               regimen text,
               regimen_start_date date,
               regimen_end_date date,
               Treatment_free_Interval int,
               Time_to_Treatment_Discontinuation int,
               Time_to_Next_Treatment int
  );
  
  INSERT INTO @cohortDatabaseSchema.@regimenStatsTable (
               cohort_definition_id,
               person_id,
               Line_of_therapy,
               regimen,
               regimen_start_date,
               regimen_end_date,
               Treatment_free_Interval,
               Time_to_Treatment_Discontinuation,
               Time_to_Next_Treatment
  )
  
  with temp_ as (SELECT DISTINCT c.cohort_definition_id,
                  c.subject_id as person_id_,
                  c.cohort_start_date, c.cohort_end_date,
                  op.observation_period_end_date,
                  d.death_date, r.*
    			  FROM @cohortDatabaseSchema.@cohortTable c
            LEFT JOIN @cohortDatabaseSchema.@regimenIngredientsTable r
                on r.person_id = c.subject_id
                and r.regimen_start_date >= DATEADD(day, -14, c.cohort_start_date)
                and r.regimen_end_date >= c.cohort_start_date
                and r.regimen_start_date <= c.cohort_end_date
            LEFT JOIN @cdmDatabaseSchema.observation_period op
                on op.person_id = c.subject_id
                and op.observation_period_start_date <= c.cohort_start_date
                and op.observation_period_end_date >= c.cohort_end_date
            LEFT JOIN @cdmDatabaseSchema.death d on d.person_id = c.subject_id
            ORDER BY c.cohort_definition_id, c.subject_id, r.regimen_start_date),
  
  
  temp_0 as(
          SELECT distinct  cohort_definition_id, person_id_ as person_id, cohort_start_date, regimen_start_date,
            coalesce(regimen_end_date, cohort_end_date,observation_period_end_date,
            death_date) as  regimen_end_date,
            regimen, observation_period_end_date, death_date , cohort_end_date,
            ingredient_end_date, ingredient_start_date
          	FROM temp_ ORDER BY 1,2,3,4
  ),
  
  
  temp_1 as (
            SELECT cohort_definition_id, person_id,
          	 max(ingredient_end_date)  regimen_end_date,
          	 regimen, regimen_start_date,
          	 ingredient_start_date, death_date,
          	 cohort_start_date
          	 FROM temp_0
          	group by cohort_definition_id, person_id,
          	cohort_start_date, death_date, regimen,
          	regimen_start_date,ingredient_start_date
          	order by 1,2,5
  	),
  
  
  temp_2 as (
        	SELECT distinct *,
        	coalesce(lag(regimen, 1) over
        	(order by person_id, regimen_start_date) != regimen, True) as New_regimen
        		FROM temp_1
        	group by cohort_definition_id, person_id,
        	 death_date, regimen, regimen_start_date,
        	 ingredient_start_date,	 regimen_end_date,
        	 cohort_start_date
        	order by 2,5
  	),
  
  
  temp_3 as (
            SELECT *,
          	case WHEN New_regimen = True then
          	 row_number() over (PARTITION BY  person_id, cohort_definition_id, New_regimen
           ORDER BY cohort_definition_id, person_id, regimen_start_date)
           end as Line_of_therapy
  FROM temp_2
        ORDER BY 2,5
   ),
  
  temp_4 as (
        	SELECT  cohort_definition_id, person_id,
        	regimen_start_date,regimen_end_date, death_date, regimen,
        	count(Line_of_therapy) over
        	(partition by person_id order by regimen_start_date)
        	as Line_of_therapy,
        	cohort_start_date
        	FROM temp_3
        	order by cohort_definition_id, person_id, regimen_start_date
  ),
  
  temp_5 as (SELECT  distinct cohort_definition_id,
        	person_id,
        	CASE WHEN Line_of_therapy = 0 then 1
        	else Line_of_therapy end as Line_of_therapy
        	,regimen,
        	min(regimen_start_date) over
        	(partition by cohort_definition_id, person_id, Line_of_therapy)
        	AS regimen_start_date,
        	max(regimen_end_date)
        	over (partition by
        	cohort_definition_id, person_id, Line_of_therapy) as  regimen_end_date,
        	cohort_start_date
        	FROM temp_4
        	order by cohort_definition_id, person_id, regimen_start_date),
  
  temp_6 as (
            SELECT cohort_definition_id,
               person_id,
               Line_of_therapy,
               regimen,
               regimen_start_date,
               regimen_end_date,
        	   case WHEN lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
              	person_id order by person_id) - regimen_end_date <= 0 then NULL
        	   else lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
              	person_id order by person_id) - regimen_end_date end
        			            as Treatment_free_Interval,
  
  
        	CASE WHEN lead(regimen_start_date, 1) over (PARTITION BY
        	               cohort_definition_id,	person_id
        	               order by cohort_definition_id,
        							   person_id) - regimen_start_date >= @gapBetweenTreatment
        							   OR lead(regimen_start_date, 1) over (PARTITION BY
        							   cohort_definition_id,person_id
        							   order by cohort_definition_id,person_id) IS NULL
        							   then abs(regimen_start_date - regimen_end_date)
        							   end as Time_to_Treatment_Discontinuation,
  
        	CASE WHEN Line_of_therapy = 1 AND
        	  lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
        	order by cohort_definition_id, person_id) IS NOT NULL AND
        	lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
        	  order by cohort_definition_id, person_id) - regimen_start_date > 0
        	   then lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
        	  order by cohort_definition_id, person_id) - regimen_start_date
        		end
        		as Time_to_Next_Treatment
        FROM temp_5
        order by  cohort_definition_id, person_id, regimen_start_date, Line_of_therapy)
  
  SELECT *
  FROM temp_6 order by 1,2,3,5
  "
  
  sqlRendered <- SqlRender::render(
    sql = sql,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    regimenStatsTable = regimenStatsTable,
    regimenIngredientsTable = regimenIngredientsTable,
    gapBetweenTreatment = gapBetweenTreatment
  )
  
  sqlTranslated <- SqlRender::translate(
    sql = sqlRendered,
    targetDialect = connectionDetails$dbms
  )
  
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sqlTranslated
  )
}


#' @returns dataframe with categories of treatment
#' @export
createCategorizedRegimensTable <- function(connectionDetails,
                                           cohortDatabaseSchema,
                                           regimenStatsTable,
                                           targetIds) {
  packageName <- getThisPackageName()
  sqlFileName <- "RegimenCategories.sql"
  pathToSql <- system.file("sql", "sql_server",
                           "TreatmentAnalysis", sqlFileName,
                           package = packageName
  )
  sql <- "
  /*
  `regimen_1EGFR tyrosine kinase inhibitors (TKI) [Erlotinib, Gefitinib, Afatinib, Dacomitinib, Osimertinib]
  regimen_2Other TKIs [Crizotinib, Ceritinib, Brigatinib, Alectinib, Lorlatinib, Entrectinib,, Capmatinib, Selpercatinib, Pralsetinib, Vandetanib, Cabozantinib, Lenvatinib, Larotrectinib, Dabrafenib+Trametinib]
  regimen_3Immune checkpoint inhibitors (anti-PD1/L1, anti-CTLA-4 or both)
  regimen_4Immune checkpoint inhibitors (anti-PD1/L1) and platinum doublet chemotherapy with or without anti-VEGF monoclonal antibody (mAb) or dual immune checkpoint inhibitors (anti-PD1 and anti-CTLA-4) and platinum doublet chemotherapy
  regimen_5Platinum doublet chemotherapy with or without anti-VEGF mAb
  regimen_6Single agent chemotherapy with or without anti-VEGF mAb [Pemetrexed with or without Bevacizumab, Docetaxel with or without Ramucirumab]*/
  WITH cte AS (SELECT cohort_definition_id,
                      person_id,
                      Line_of_therapy,
                      regimen,
                      (CASE WHEN regimen in ('erlotinib',
                                            'gefitinib',
                                            'afatinib',
                                            'dacomitinib',
                                            'osimertinib')
                              then 1 else 0 end) AS EGFR_tyrosine_kinase_inhibitors,
  
                      (CASE WHEN regimen in ('crizotinib', 'ceritinib',
                                'brigatinib', 'alectinib',
                                'lorlatinib', 'entrectinib',
                                'capmatinib', 'selpercatinib',
                                'pralsetinib', 'vandetanib',
                                'cabozantinib', 'lenvatinib',
                                'larotrectinib') OR regimen like ('%dabrafenib%trametinib%')
                            then 1 else 0 end) AS Other_EGFR_tyrosine_kinase_inhibitors,
  
                     ( CASE WHEN regimen in ('pembrolizumab', 'nivolumab', 'dostarlimab') then 1
                            else 0 end) AS Anti_PD_1,
  
                      (CASE  WHEN regimen in ('atezolizumab', 'avelumab', 'durvalumab') then 1
                            else 0 end) AS Anti_L_1,
  
                      (CASE WHEN regimen in ('ipilimumab') then 1 else 0 end) AS Anti_CTLA_4,
  
                      (CASE  WHEN regimen in ('cisplatin', 'carboplatin') then 1
                            else 0 end) AS Platinum_doublet,
  
                      (CASE  WHEN regimen in ('%pemetrexed%docetaxel%') then 1
                            else 0 end) AS Single_agent,
  
                      (CASE  WHEN regimen in ('bevacizumab','ranibizumab','aflibercept','ramucirumab')
                            then 1 else 0 end) AS anti_VEGF_mAb
  
             FROM @cohortDatabaseSchema.@regimenStatsTable
             WHERE cohort_definition_id IN (@targetIds)
             ORDER BY 1, 3, 2
  )
  
  SELECT cohort_definition_id,
         person_id,
         Line_of_therapy,
  
        (CASE WHEN EGFR_tyrosine_kinase_inhibitors = 1 AND
        Other_EGFR_tyrosine_kinase_inhibitors +
        Anti_PD_1 + Anti_L_1 + Platinum_doublet + Single_agent + anti_VEGF_mAb = 0
          then  'TKI'
  
        WHEN Other_EGFR_tyrosine_kinase_inhibitors = 1 AND EGFR_tyrosine_kinase_inhibitors +
        Anti_PD_1 + Anti_L_1 + Platinum_doublet + Single_agent + anti_VEGF_mAb = 0
          then  'Other_TKIs'
  
        WHEN Platinum_doublet = 1 AND Anti_PD_1 + Platinum_doublet +
        anti_VEGF_mAb >= 2 OR  Anti_L_1  + Platinum_doublet +
        anti_VEGF_mAb >= 2 OR Platinum_doublet + Anti_CTLA_4 +  anti_VEGF_mAb > 2
        AND Other_EGFR_tyrosine_kinase_inhibitors + EGFR_tyrosine_kinase_inhibitors +
        Single_agent = 0
          then 'anti-PD1/L1_and_Platinum_doublet'
  
        WHEN Platinum_doublet + anti_VEGF_mAb = 0 AND Anti_PD_1 + Anti_L_1
        + Anti_CTLA_4 >= 2 AND Other_EGFR_tyrosine_kinase_inhibitors +
        EGFR_tyrosine_kinase_inhibitors + Single_agent = 0
        then 'Immune_checkpoint_inhibitors'
  
        WHEN Platinum_doublet + anti_VEGF_mAb >= 1 AND Anti_PD_1 + Anti_L_1
        + Anti_CTLA_4 = 0 AND  Platinum_doublet = 1 AND Other_EGFR_tyrosine_kinase_inhibitors +
        EGFR_tyrosine_kinase_inhibitors + Single_agent = 0
        then 'Platinum_doublet'
  
        WHEN Single_agent + anti_VEGF_mAb >= 1 AND Single_agent = 1
        AND Other_EGFR_tyrosine_kinase_inhibitors +
        EGFR_tyrosine_kinase_inhibitors + Platinum_doublet + Anti_CTLA_4
        + Anti_PD_1 + Anti_L_1 = 0
        then 'Single_agent_chemotherapy'
  
        else 'Other' end) AS Regimens_categories
  
  FROM cte
  ORDER BY 1, 3, 2, 4;
  "
  
  sqlTmp <- SqlRender::render(
    sql = sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIds
  )
  
  sqlTmp <- SqlRender::translate(
    sql = sqlTmp,
    targetDialect = connectionDetails$dbms
  )
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  as.data.frame(DatabaseConnector::querySql(
    connection = connection,
    sql = sqlTmp,
    snakeCaseToCamelCase = T
  ))
}
