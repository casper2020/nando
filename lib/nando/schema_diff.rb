module NandoSchemaDiff

  def self.diff_schemas (source_schema, target_schema)
    db_connection = NandoMigrator.get_database_connection();

    # results = db_connection.exec("\\d") # DOES NOT WORK

    results = db_connection.exec("
       SELECT COALESCE(c1.table_name, c2.table_name) AS table_name,
              COALESCE(c1.column_name, c2.column_name) AS table_column,
              COALESCE(c1.table_schema, c2.table_schema) AS table_schema,
              COALESCE(c1.data_type, c2.data_type) AS data_type,
              c1.column_name AS schema1,
              c2.column_name AS schema2
         FROM
              (SELECT table_name, column_name, table_schema, data_type
                 FROM information_schema.columns c
                WHERE c.table_schema = '#{source_schema}') c1
              FULL JOIN
              (SELECT table_name, column_name, table_schema, data_type
                 FROM information_schema.columns c
                WHERE c.table_schema = '#{target_schema}') c2
              ON c1.table_name = c2.table_name AND c1.column_name = c2.column_name
        WHERE c1.column_name IS NULL OR c2.column_name IS NULL
        ORDER by table_schema, table_name, table_column;
    ")

    source = get_schema_structure(source_schema)
    target = get_schema_structure(target_schema)

    source_info = get_info_structure()
    target_info = get_info_structure()

    # compare structure
    # checking for different tables
    if keys_diff1 = source.keys - target.keys
      source_info['tables']['missing'] += keys_diff1
    end

    if keys_diff = target.keys - source.keys
      target_info['tables']['missing'] += keys_diff
    end

    source.keys.each do |table|
      # if keys_diff1.include?(table)
      #   next
      # end

      if keys_diff = source[table]['columns'].keys - target[table]['columns'].keys
        source_info['columns']['missing'] += keys_diff
      end
    end




    # debugger

    print_diff_info(source_info, source_schema)
    print_diff_info(target_info, target_schema)
  end

  def self.get_schema_structure (curr_schema)
    schema_structure = {}
    db_connection = NandoMigrator.get_database_connection();

    # get all tables in a schema
    results = db_connection.exec("
      SELECT *
        FROM information_schema.tables
       WHERE table_schema = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['table_name']] = { 'columns' => {}, 'indexes' => {} }
    end

    results = db_connection.exec("
      SELECT *
        FROM information_schema.columns
       WHERE table_schema = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['table_name']]['columns'][row['column_name']] = {}
    end

    return schema_structure
  end

  def self.get_info_structure
    return {
      'tables' => { 'missing' => [] },
      'columns' => { 'missing' => [] }
    }
  end

  def self.print_diff_info (info, schema)
    info['tables']['missing'].each do |table|
      _warn "Table '#{table}' does not exist in schema '#{schema}'", 'Diff'
    end

    # _warn "Table '#{row['table_name']}' in schema '#{schema}' does not have a column '#{row['table_column']}' of type '#{row['data_type']}'", 'Diff'
    info['columns']['missing'].each do |column|
      _warn "Column '#{column}' does not exist in schema '#{schema}'", 'Diff' # TODO: fix this message
    end

  end

end