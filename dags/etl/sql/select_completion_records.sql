SELECT Id, LearnerId, CompletionDate 
FROM CompletionRecord 
WHERE Updated >= {{ds}} AND Updated < {{next_ds}}
ORDER BY CompletionDate