insert into target.reasons (
    id,
    team_area,
    reason_code_category,
    reason_code,
    category,
    description,
    how_to_dispute
)
    select 
        _line as id,
        trim(team_area) as team_area,
        trim(reason_code_category) as reason_code_category, 
        trim(reason_codes) as reason_code,
        trim(category) as category,
        description as description,
        how_to_dispute as how_to_dispute
    from pc_fivetran_db.s2s_target_reasons.target_reason_codes_reasons
;