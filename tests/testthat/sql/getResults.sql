{TYPEC INT[] @cohort_id}
{TYPEC TEXT @cohort}

SELECT * FROM @result_schema.@cohort where cohort_id IN (@cohort_id)