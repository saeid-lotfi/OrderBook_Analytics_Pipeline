CREATE TABLE IF NOT EXISTS {database}.dead_letter_events
AS {database}.dead_letter_events_local
ENGINE = Distributed(
    '{cluster}',
    '{database}',
    'dead_letter_events_local',
    cityHash64(raw_message)
);