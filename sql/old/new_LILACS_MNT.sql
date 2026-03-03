SELECT 'LILACS_MNT' AS dbase,
      a.`id`,
      b.`title_monographic`,
      SUBSTRING(a.`publication_date_normalized`,1,4) AS publication_year,
      IFNULL(b.`individual_author_monographic`,b.`corporate_author_monographic`) AS author,
      b.`pages_monographic`,
      b.`volume_monographic`,
      b.`issue_number`,
      a.`cooperative_center_code`,
      a.`literature_type`,
      a.`treatment_level`,
      a.`status`,
      a.`electronic_address`,
      CONCAT('https://fi-admin.bvsalud.org/bibliographic/edit-source/',b.`reference_ptr_id`) AS link_fiadmin
FROM
     `biblioref_reference` AS a,
     `biblioref_referencesource` AS b
WHERE (LEFT(a.`literature_type`,1) = 'M'
OR LEFT(a.`literature_type`,1) = 'N'
OR LEFT(a.`literature_type`,1) = 'T')
AND a.`id` = b.`reference_ptr_id`;
