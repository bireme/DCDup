SELECT 'LILACS_MNT' AS dbase,
      a.`id`,
      b.`title_monographic`,
      SUBSTRING(a.`publication_date_normalized`,1,4) AS publication_year,
      IFNULL(b.`individual_author_monographic`,b.`corporate_author_monographic`) AS author,
      b.`pages_monographic`           
FROM
     `biblioref_reference` AS a,
     `biblioref_referencesource` AS b
WHERE (LEFT(a.`literature_type`,1) = 'M'
OR LEFT(a.`literature_type`,1) = 'N'
OR LEFT(a.`literature_type`,1) = 'T')
AND a.`id` = b.`reference_ptr_id`;
