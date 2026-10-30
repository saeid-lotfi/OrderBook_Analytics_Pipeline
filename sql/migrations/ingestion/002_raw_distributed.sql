CREATE TABLE IF NOT EXISTS {database}.raw_events
AS {database}.raw_events_local
ENGINE = Distributed(
    '{cluster}',
    '{database}',
    'raw_events_local',
    cityHash64(
        if(
            isValidJSON(raw_message),
            if(
                JSONExtractUInt(raw_message, 'id') > 0,
                toString(JSONExtractUInt(raw_message, 'id')),
                raw_message
            ),
            raw_message
        )
    )
);