SELECT DISTINCT LILACS_original_id, title_serial, volume_serial FROM biblioref_reference, biblioref_referencesource WHERE  title_serial <> '' AND literature_type = 'S'
