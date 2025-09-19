{TYPEC INT[] @cohort_id}
{TYPEC TEXT @cohort}
{TYPEC TEXT @result_schema}

SELECT * FROM @result_schema.@cohort where cohort_definition_id IN (@cohort_id)