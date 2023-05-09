SELECT 'LILACS_Sas' AS dbase,
       CONCAT_WS('-','fiadmin',b.`reference_ptr_id`),
       b.`english_translated_title`,
       c.`title_serial`,
       SUBSTRING(a.`publication_date_normalized`,1,4) AS publication_year,
       c.`volume_serial`,
       c.`issue_number`
FROM `biblioref_referenceanalytic` AS b
INNER JOIN `biblioref_referencesource` AS c
ON b.`source_id` = c.`reference_ptr_id`
AND b.`english_translated_title` <> ''
AND b.`english_translated_title` <> 'x'
INNER JOIN `biblioref_reference` AS a
ON b.`source_id` = a.`id` AND LEFT(a.`literature_type`,1) = 'S';

