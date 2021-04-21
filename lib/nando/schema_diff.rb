module NandoSchemaDiff

  def self.diff_schemas (source_schema, target_schema)
    db_connection = NandoMigrator.get_database_connection();

    # results = db_connection.exec("\\d") # DOES NOT WORK

    source_info = get_info_base_structure()
    target_info = get_info_base_structure()

    source = get_schema_structure(source_schema)
    target = get_schema_structure(target_schema)

    # start comparing structure

    # checking for different tables
    check_different_tables(source, target, source_info, target_info)
    check_different_tables(target, source, target_info, source_info)

    # checking for different columns in all shared tables
    check_mismatching_columns(source, target, source_info, target_info)
    check_mismatching_columns(target, source, target_info, source_info)

    # checking for mismatching columns in all shared tables



    # checking for different indexes in all shared tables
    # checking for mismatching indexes in all shared tables

    # checking for different triggers in all shared tables
    # checking for mismatching triggers in all shared tables

    # checking for different constraints in all shared tables
    # checking for mismatching constraints in all shared tables


    # TODO: what to do about views, types, etc

    puts ""
    print_diff_info(source_info, source_schema)
    puts ""
    print_diff_info(target_info, target_schema)
    puts ""
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

  def self.get_info_base_structure
    return {
      'tables' => {
        'missing' => [],
        'extra' => []
      },
      'columns' => {
        'missing' => [],
        'extra' => []
      }
    }
  end

  def self.check_different_tables (left_schema, right_schema, left_info, right_info)
    if keys_diff = left_schema.keys - right_schema.keys
      left_info['tables']['extra'] += keys_diff
    end

    if keys_diff = right_schema.keys - left_schema.keys
      left_info['tables']['missing'] += keys_diff
    end
  end

  def self.check_mismatching_columns (left_schema, right_schema, left_info, right_info)
    left_schema.keys.each do |table|
      if left_info['tables']['missing'].include?(table) || right_info['tables']['missing'].include?(table)
        _debug "Skipping: #{table}"
        next
      end

      if keys_diff = left_schema[table]['columns'].keys - right_schema[table]['columns'].keys
        left_info['columns']['missing'] += keys_diff
      end
    end
  end

  def self.print_diff_info (info, schema)
    _warn "START PRINTING '#{schema}'"

    info['tables']['missing'].each do |table|
      _warn "Table '#{table}' does not exist in schema '#{schema}'", 'Diff'
    end

    # _warn "Table '#{row['table_name']}' in schema '#{schema}' does not have a column '#{row['table_column']}' of type '#{row['data_type']}'", 'Diff'
    info['columns']['missing'].each do |column|
      _warn "Column '#{column}' does not exist in schema '#{schema}'", 'Diff' # TODO: fix this message
    end

  end

end