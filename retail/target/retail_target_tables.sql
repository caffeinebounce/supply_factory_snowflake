create or replace table target.reasons (
    id varchar(50),
    team_area varchar(255),
    reason_code_category varchar(255), 
    reason_code varchar(255),
    category varchar(1024),
    description varchar(2048),
    how_to_dispute varchar(2048)
);