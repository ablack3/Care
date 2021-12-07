connectionDetails <- createConnectionDetails(dbms = "postgresql", 
    user = "me", password = "secret", server = "example.com/datasource", 
    port = "5432", extraSettings = NULL, oracleDriver = "thin", 
    pathToDriver = "/Users/adamblack/jdbc_drivers")

vocabularyDatabaseSchema <- "vocabulary"
oracleTempSchema <- NULL
connection <- connect(connectionDetails = connectionDetails)

cid13 <- c(21601390L, 4181511L)
cid16 <- 258369L
cid17 <- 1350504L
cid18 <- 432851L
nm13 <- "antineoplastic agents"
nm16 <- "Lung Cancer"
nm17 <- "Etoposide"
nm18 <- "Secondary malignant neoplastic disease"
conceptMapping13 <- list(list(includeDescendants = TRUE, isExcluded = TRUE, 
    includeMapped = TRUE), list(includeDescendants = TRUE, isExcluded = TRUE, 
    includeMapped = TRUE))
conceptMapping16 <- list(list(includeDescendants = TRUE, isExcluded = TRUE, 
    includeMapped = TRUE))
conceptMapping17 <- list(list(includeDescendants = TRUE, isExcluded = TRUE, 
    includeMapped = TRUE))
conceptMapping18 <- list(list(includeDescendants = TRUE, isExcluded = TRUE, 
    includeMapped = TRUE))
conceptSet13 <- getConceptIdDetails(conceptIds = cid13, connection = connection, 
    connectionDetails = NULL, vocabularyDatabaseSchema = vocabularyDatabaseSchema, 
    oracleTempSchema = oracleTempSchema, mapToStandard = FALSE) %>% 
    createConceptSetExpressionCustom(Name = nm13, conceptMapping = conceptMapping13)
conceptSet16 <- getConceptIdDetails(conceptIds = cid16, connection = connection, 
    connectionDetails = NULL, vocabularyDatabaseSchema = vocabularyDatabaseSchema, 
    oracleTempSchema = oracleTempSchema, mapToStandard = FALSE) %>% 
    createConceptSetExpressionCustom(Name = nm16, conceptMapping = conceptMapping16)
conceptSet17 <- getConceptIdDetails(conceptIds = cid17, connection = connection, 
    connectionDetails = NULL, vocabularyDatabaseSchema = vocabularyDatabaseSchema, 
    oracleTempSchema = oracleTempSchema, mapToStandard = FALSE) %>% 
    createConceptSetExpressionCustom(Name = nm17, conceptMapping = conceptMapping17)
conceptSet18 <- getConceptIdDetails(conceptIds = cid18, connection = connection, 
    connectionDetails = NULL, vocabularyDatabaseSchema = vocabularyDatabaseSchema, 
    oracleTempSchema = oracleTempSchema, mapToStandard = FALSE) %>% 
    createConceptSetExpressionCustom(Name = nm18, conceptMapping = conceptMapping18)
attPC1_1 <- createDrugTypeExcludeAttribute(logic = FALSE)
attrListPC1 <- list(attPC1_1)
queryPC1 <- createDrugExposure(conceptSetExpression = conceptSet13, 
    attributeList = attrListPC1)
attPC2_1 <- createProcedureTypeExcludeAttribute(logic = FALSE)
attrListPC2 <- list(attPC2_1)
queryPC2 <- createProcedureOccurrence(conceptSetExpression = conceptSet13, 
    attributeList = attrListPC2)
PrimaryCriteria <- createPrimaryCriteria(Name = "cohortPrimaryCriteria", 
    ComponentList = list(queryPC1, queryPC2), ObservationWindow = createObservationWindow(PriorDays = 365L, 
        PostDays = 0L), Limit = "First")
attACgrp_1_1 <- createConditionTypeExcludeAttribute(logic = FALSE)
attrListACgrp_1 <- list(attACgrp_1_1)
queryACgrp_1 <- createConditionOccurrence(conceptSetExpression = conceptSet18, 
    attributeList = attrListACgrp_1)
timelineACgrp_1 <- createTimeline(StartWindow = createWindow(StartDays = 180L, 
    StartCoeff = "Before", EndDays = 0L, EndCoeff = "After", 
    EventStarts = TRUE, IndexStart = TRUE), EndWindow = NULL, 
    RestrictVisit = FALSE, IgnoreObservationPeriod = FALSE)
countACgrp_1 <- createCount(Query = queryACgrp_1, Logic = "at_least", 
    Count = 1L, isDistinct = FALSE, Timeline = timelineACgrp_1)
ACgrp <- createGroup(Name = "ACgrp", type = "ALL", count = NULL, 
    criteriaList = list(countACgrp_1), demographicCriteriaList = NULL, 
    Groups = NULL)
AdditionalCriteria <- createAdditionalCriteria(Name = "cohortAdditionalCriteria", 
    Contents = ACgrp, Limit = "First")
attInclusionRule1_1_1 <- createConditionTypeExcludeAttribute(logic = FALSE)
attrListInclusionRule1_1 <- list(attInclusionRule1_1_1)
queryInclusionRule1_1 <- createConditionOccurrence(conceptSetExpression = conceptSet16, 
    attributeList = attrListInclusionRule1_1)
timelineInclusionRule1_1 <- createTimeline(StartWindow = createWindow(StartDays = "All", 
    StartCoeff = "Before", EndDays = 0L, EndCoeff = "After", 
    EventStarts = TRUE, IndexStart = TRUE), EndWindow = NULL, 
    RestrictVisit = FALSE, IgnoreObservationPeriod = FALSE)
countInclusionRule1_1 <- createCount(Query = queryInclusionRule1_1, 
    Logic = "at_least", Count = 1L, isDistinct = FALSE, Timeline = timelineInclusionRule1_1)
attDemCrit_InclusionRule1_1_1 <- createAgeAttribute(Op = "gte", 
    Value = 18L, Extent = NULL)
InclusionRule1 <- createGroup(Name = "LungCancer", type = "ALL", 
    count = NULL, criteriaList = list(countInclusionRule1_1), 
    demographicCriteriaList = list(attDemCrit_InclusionRule1_1_1), 
    Groups = NULL)
attInclusionRule2_1_1 <- createDrugTypeExcludeAttribute(logic = FALSE)
attrListInclusionRule2_1 <- list(attInclusionRule2_1_1)
queryInclusionRule2_1 <- createDrugExposure(conceptSetExpression = conceptSet17, 
    attributeList = attrListInclusionRule2_1)
timelineInclusionRule2_1 <- createTimeline(StartWindow = createWindow(StartDays = 0L, 
    StartCoeff = "Before", EndDays = "All", EndCoeff = "After", 
    EventStarts = TRUE, IndexStart = TRUE), EndWindow = NULL, 
    RestrictVisit = FALSE, IgnoreObservationPeriod = FALSE)
countInclusionRule2_1 <- createCount(Query = queryInclusionRule2_1, 
    Logic = "exactly", Count = 0L, isDistinct = FALSE, Timeline = timelineInclusionRule2_1)
InclusionRule2 <- createGroup(Name = "No Etoposide", type = "ANY", 
    count = NULL, criteriaList = list(countInclusionRule2_1), 
    demographicCriteriaList = NULL, Groups = NULL)
InclusionRules <- createInclusionRules(Name = "cohortInclusionRules", 
    Contents = list(InclusionRule1, InclusionRule2), Limit = "First")
CohortEra <- createCohortEra(EraPadDays = 0L, LeftCensorDate = NULL, 
    RightCensorDate = NULL)
CohortDefinition <- createCohortDefinition(Name = "CohortDefinition", 
    cdmVersionRange = ">=5.0.0", PrimaryCriteria = PrimaryCriteria, 
    AdditionalCriteria = AdditionalCriteria, InclusionRules = InclusionRules, 
    EndStrategy = NULL, CensoringCriteria = NULL, CohortEra = CohortEra)
