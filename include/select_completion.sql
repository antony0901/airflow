select %(window_start_date)s;
SELECT Count(Id) from CompletionRecord WHERE Updated > %(window_start_date)s;