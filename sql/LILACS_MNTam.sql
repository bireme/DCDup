SELECT 'LILACS_MNTam' AS dbase,
       a.`id`,
       b.`title`,
       c.`title_monographic`,
       SUBSTRING(a.`publication_date_normalized`,1,4) AS publication_year,
       IFNULL(b.`individual_author`,b.`corporate_author`) AS author,
       b.`pages`
FROM `biblioref_reference` AS a,
     `biblioref_referenceanalytic` AS b,
     `biblioref_referencesource` AS c
WHERE LEFT(a.`literature_type`,1) <> 'S'
AND LEFT(a.`treatment_level`,1) = 'a'
AND a.`id` = b.`reference_ptr_id`
AND b.`source_id` = c.`reference_ptr_id`;
