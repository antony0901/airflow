selet * from CompletionRecord
where updated >= %(window_start_date)s 
and updated < %(window_end_date)s