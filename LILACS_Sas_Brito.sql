SELECT a.`id`,
       b.`reference_ptr_id`,
       a.`literature_type`,
       a.`treatment_level`,
       a.`reference_title`,
       a.`publication_date_normalized`,
       b.`title`,
       b.`english_translated_title`,
       b.`individual_author`,
       b.`corporate_author`,
       b.`pages`,
       b.`source_id`,
       c.`title_serial`,
       c.`volume_serial`,
       c.`issue_number`
FROM `biblioref_reference` AS a,
     `biblioref_referenceanalytic` AS b,
     `biblioref_referencesource` as c
WHERE left(a.`literature_type`,1) = 'S'
AND a.`id` = b.`reference_ptr_id`
and b.`source_id` = c.`reference_ptr_id`
and a.`treatment_level` = 'as';

