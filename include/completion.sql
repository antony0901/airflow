SELECT cr.Id as CompletionRecordId, cr.CompletionDate, crp.Discriminator as LearningAsset, cr.LearnerId,
	-- activity ID--
	GROUP_CONCAT(
		DISTINCT 
		CASE WHEN crp.Discriminator = 'CompletionRecordPropertyLCV2' THEN 
			case when crp.KaplanUID is null then crp.LearningContentId else crp.KaplanUID end
		WHEN crp.Discriminator = 'CompletionRecordPropertySLV2' THEN 
			case when crp.SLV1Id is null then crp.SLId else crp.SLV1Id end
		WHEN crp.Discriminator = 'CompletionRecordPropertyShortCourseV2' THEN crp.ShortCourseId
		ELSE crp.KPECId 
		END
	) as ActivityID,
	-- activity Name--
	GROUP_CONCAT(
		DISTINCT 
		CASE WHEN crp.Discriminator = 'CompletionRecordPropertyLCV2' THEN crm.Title
		WHEN crp.Discriminator = 'CompletionRecordPropertySLV2' THEN crp.ActivityName
		WHEN crp.Discriminator = 'CompletionRecordPropertyShortCourseV2' THEN ''
		ELSE crm.CourseName 
		END
	) as ActivityName,
	-- Activity Type--
	crm.ActivityType,
	-- Provider Name--
	GROUP_CONCAT(
		DISTINCT 
		CASE WHEN crp.Discriminator = 'CompletionRecordPropertyLCV2' THEN crm.Title
		WHEN crp.Discriminator = 'CompletionRecordPropertySLV2' THEN crp.ActivityName
		WHEN crp.Discriminator = 'CompletionRecordPropertyShortCourseV2' THEN ''
		ELSE crm.CourseName 
		END
	) as ProviderName,
	-- Learning Structure--
	crm.LearningStructure,
	-- AQF --
	crm.AQFLevel,
	-- Delivery Type--
	GROUP_CONCAT(
		DISTINCT 
		CASE WHEN crp.Discriminator = 'CompletionRecordPropertyLCV2' THEN crm.CompletionRecordMetadataLCV2_DeliveryType 
		WHEN crp.Discriminator = 'CompletionRecordPropertySLV2' THEN crm.CompletionRecordMetadataSLV2_DeliveryType 
		WHEN crp.Discriminator = 'CompletionRecordPropertyShortCourseV2' THEN ''
		ELSE crm.DeliveryType
		END
	) as DeliveryType,
	-- Learning Type--
	GROUP_CONCAT(
		DISTINCT 
		CASE WHEN crp.Discriminator = 'CompletionRecordPropertyLCV2' THEN crm.CompletionRecordMetadataLCV2_DeliveryType
		WHEN crp.Discriminator = 'CompletionRecordPropertySLV2' THEN crm.CompletionRecordMetadataSLV2_LearningType
		WHEN crp.Discriminator = 'CompletionRecordPropertyShortCourseV2' THEN ''
		ELSE crm.LearningType 
		END
	) as LearningType,
	-- Topic ID --
	GROUP_CONCAT(
		DISTINCT 
		CASE WHEN crp.Discriminator = 'CompletionRecordPropertyLCV2' THEN crt.CompletionRecordTopicLCV2_TopicId
		WHEN crp.Discriminator = 'CompletionRecordPropertySLV2' THEN crt.CompletionRecordTopicSLV2_TopicId
		WHEN crp.Discriminator = 'CompletionRecordPropertyShortCourseV2' THEN crt.CompletionRecordTopicShortCourseV2_TopicId
		ELSE crt.TopicId 
		END
	) as TopicId,
	SUM(case when crt.IsTopic then crt.Minutes else 0 end)  as TotalMinutes,
	SUM(case when crt.IsTopic then ROUND(crt.Minutes/60, 2) else 0 end)  as TotalHours,
	SUM(case when crt.IsTopic then crt.Points else 0 end)  as TotalPoint,
	-- Accreditation --
	GROUP_CONCAT(
		concat(
		case when cra.AccreditationName is null then 'NULL' ELSE  cra.AccreditationName end
		, '___', 
		case when cra.AccrediationAbbr is null then 'NULL' ELSE  cra.AccrediationAbbr end
		, '___', cra.Minutes, '___', cra.Points)
	) as Accreditation
FROM CompletionRecord cr
INNER join CompletionRecordProperties crp on cr.Id = crp.CompletionRecordId
INNER join CompletionRecordMetadata crm force index (IX_CompletionRecordMetadata_CompletionRecordPropertyId) on crp.Id = crm.CompletionRecordPropertyId 
INNER JOIN CompletionRecordTopics crt on crp.Id = crt.CompletionRecordPropertyId
LEFT JOIN CompletionRecordAccreditations cra on crp.Id =cra.CompletionRecordPropertyId
group by cr.Id