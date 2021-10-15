-- TODO: Write the purpose and output of this SQL script in plain english

/*
Parameters
@writeDatabaseSchema
@cohortTable
@regimenTable
@drug_classification_id_input - The most important parameter to this script. Defines what eras are captured.
*/

DROP TABLE IF EXISTS @writeDatabaseSchema.@cohortTable;
DROP TABLE IF EXISTS @writeDatabaseSchema.@regimenTable;

-- QUESTION: should we be using bigint?

-- DDL for the regimen table
CREATE TABLE @writeDatabaseSchema.@regimenTable (
       concept_name varchar(max),
       drug_era_id bigint,
       person_id bigint not null, 
       rownum bigint,
       drug_concept_id bigint, 
       ingredient_start_date date not null,
       ingredient_end_date date
);

-- DDL for the cohort table
CREATE TABLE @writeDatabaseSchema.@cohortTable (
       concept_name varchar(max),
       drug_era_id bigint,
       person_id bigint not null, 
       rownum bigint,
       drug_concept_id bigint, 
       ingredient_start_date date not null,
       ingredient_end_date date
);

-- TODO: write in plain english what this query should return.
-- get all ancestors of all drug_era concepts. Keep drug era rows where an ancestor 
-- of the drug_era concept is a descendant of the drug_classification_id_input
insert into @writeDatabaseSchema.@cohortTable (
  with onc_drug_eras as (
    select 
      lower(c.concept_name) as concept_name,
      de.drug_era_id, 
      de.person_id, 
      de.drug_concept_id, 
      de.drug_era_start_date as ingredient_start_date,
      de.drug_era_end_date as ingredient_end_date
    from @cdmDatabaseSchema.drug_era de 
    -- CHECK: not sure if next two lines are actually necessary since drug era is supposed to be ingredients
    inner join @cdmDatabaseSchema.concept_ancestor ca on ca.descendant_concept_id = de.drug_concept_id
    inner join @cdmDatabaseSchema.concept c on c.concept_id = ca.ancestor_concept_id
    where 
      c.concept_id in (
        -- all descendants of the classification concept
        select descendant_concept_id as drug_concept_id 
        from @cdmDatabaseSchema.concept_ancestor ca1
        where ancestor_concept_id in (@drug_classification_id_input) /* Drug concept_id  */ 
      )
      and c.concept_class_id = 'Ingredient'
  )
  select cs.concept_name,
         cs.drug_era_id,
         cs.person_id , 
         c2.rownum,
         cs.drug_concept_id, 
         cs.ingredient_start_date,
         cs.ingredient_end_date 
  from onc_drug_eras cs
  inner join (
    select person_id, row_number() over(order by person_id) as rownum 
    from (SELECT distinct person_id FROM onc_drug_eras) cs
  ) c2 
  on c2.person_id = cs.person_id
);

-- copy regimens to the regimen table
-- QUESTION: Why create a copy of the same table?
insert into  @writeDatabaseSchema.@regimenTable (
  select * 
  from @writeDatabaseSchema.@cohortTable
);




