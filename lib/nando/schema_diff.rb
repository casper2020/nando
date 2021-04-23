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
    check_different_columns(source, target, source_info, target_info)
    check_different_columns(target, source, target_info, source_info)

    # checking for mismatching columns in all shared tables
    check_mismatching_columns(source, target, source_info, target_info)
    check_mismatching_columns(target, source, target_info, source_info)


    # checking for different triggers in all shared tables
    check_different_triggers(source, target, source_info, target_info)
    check_different_triggers(target, source, target_info, source_info)

    # checking for mismatching triggers in all shared tables

    # checking for different constraints in all shared tables
    # checking for mismatching constraints in all shared tables

    # checking for different indexes in all shared tables
    # checking for mismatching indexes in all shared tables

    # TODO: what to do about views, types, etc

    puts ""
    print_diff_info(source_info, source_schema, target_schema)
    puts ""
    print_diff_info(target_info, target_schema, source_schema)
    puts ""
  end

  def self.get_schema_structure (curr_schema)
    schema_structure = {}
    db_connection = NandoMigrator.get_database_connection();

    # TODO: reduce these SELECT * to specific columns

    # get all tables in a schema
    results = db_connection.exec("
      SELECT *
        FROM information_schema.tables
       WHERE table_schema = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['table_name']] = {
        'columns' => {},
        'triggers' => {},
        'constraints' => {},
        'indexes' => {}
      }
    end

    # get all columns for each table
    results = db_connection.exec("
      SELECT *
        FROM information_schema.columns
       WHERE table_schema = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['table_name']]['columns'][row['column_name']] = {
        'ordinal_position' => row['ordinal_position'],
        'column_default'   => row['column_default'].nil? ? row['column_default'] : row['column_default'].gsub(curr_schema, ''), # remove the schema, since sequences include it in their name
        'is_nullable'      => row['is_nullable'],
        'data_type'        => row['data_type']
      }
    end

    # get all triggers for each table
    results = db_connection.exec("
      SELECT *
        FROM information_schema.triggers
       WHERE event_object_schema = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['event_object_table']]['triggers'][row['trigger_name']] = {
        'event_manipulation'  => row['event_manipulation'],
        'action_order'        => row['action_order'],
        'action_condition'    => row['action_condition'],
        'action_statement'    => row['action_statement'],
        'action_orientation'  => row['action_orientation'],
        'action_timing'       => row['action_timing']
      }
    end

    # information_schema.check_constraints
    # information_schema.views

    return schema_structure
  end

  def self.get_info_base_structure
    return {
      'tables' => {
        'missing' => [],
        'extra' => [],
        'mismatching' => {}
      },
      'views' => {
        # not currently used, but reserved for now
      }
    }
  end

  def self.setup_table_info (info, table_name)
    # create table structure if one does not exist
    if info['tables']['mismatching'][table_name].nil?
      info['tables']['mismatching'][table_name] = {
        "columns" => {
          "missing" => [],
          "extra" => [],
          "mismatching" => [] # TODO: replace with hash
        },
        "indexes" => {
          "missing" => [],
          "extra" => [],
          "mismatching" => {}
        },
        "triggers" => {
          "missing" => [],
          "extra" => [],
          "mismatching" => {}
        },
        "constraints" => {
          "missing" => [],
          "extra" => [],
          "mismatching" => {}
        }
      }
    end
  end


  # table comparison
  def self.check_different_tables (left_schema, right_schema, left_info, right_info)
    if keys_diff = left_schema.keys - right_schema.keys
      left_info['tables']['extra'] += keys_diff
    end

    if keys_diff = right_schema.keys - left_schema.keys
      left_info['tables']['missing'] += keys_diff
    end
  end


  # column comparison
  def self.check_different_columns (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info['tables']['missing'].include?(table_key) || right_info['tables']['missing'].include?(table_key)
        _debug "Skipping table (1): #{table_key}"
        next
      end

      if keys_diff = left_schema[table_key]['columns'].keys - right_schema[table_key]['columns'].keys
        setup_table_info(left_info, table_key)
        left_info['tables']['mismatching'][table_key]['columns']['extra'] += keys_diff
      end

      if keys_diff = right_schema[table_key]['columns'].keys - left_schema[table_key]['columns'].keys
        setup_table_info(left_info, table_key)
        left_info['tables']['mismatching'][table_key]['columns']['missing'] += keys_diff
      end
    end
  end

  def self.check_mismatching_columns (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info['tables']['missing'].include?(table_key) || right_info['tables']['missing'].include?(table_key)
        _debug "Skipping table (2): #{table_key}"
        next
      end

      table_value['columns'].each do |column_key, column_value|
        # ignore columns that only appear in one of the tables
        if left_info['tables']['mismatching'][table_key]['columns']['missing'].include?(column_key) || right_info['tables']['mismatching'][table_key]['columns']['missing'].include?(column_key)
          _debug "Skipping column: #{column_key}"
          next
        end

        if left_schema[table_key]['columns'][column_key] != right_schema[table_key]['columns'][column_key]
          setup_table_info(left_info, table_key)
          left_info['tables']['mismatching'][table_key]['columns']['mismatching'] << column_key # TODO: add more info, not just a key
        end
      end
    end
  end


  # trigger comparison
  def self.check_different_triggers (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info['tables']['missing'].include?(table_key) || right_info['tables']['missing'].include?(table_key)
        _debug "Skipping table (3): #{table_key}"
        next
      end

      if keys_diff = left_schema[table_key]['triggers'].keys - right_schema[table_key]['triggers'].keys
        setup_table_info(left_info, table_key)
        left_info['tables']['mismatching'][table_key]['triggers']['extra'] += keys_diff
      end

      if keys_diff = right_schema[table_key]['triggers'].keys - left_schema[table_key]['triggers'].keys
        setup_table_info(left_info, table_key)
        left_info['tables']['mismatching'][table_key]['triggers']['missing'] += keys_diff
      end
    end
  end

  def self.print_diff_info (info, source_schema, target_schema)
    _warn "START PRINTING '#{source_schema}'"

    info['tables']['missing'].each do |table|
      _warn "Table '#{table}' does not exist in schema '#{source_schema}'", 'Diff1'
    end

    info['tables']['extra'].each do |table|
      _warn "Table '#{table}' exists in '#{source_schema}' but does not exist in schema '#{target_schema}'", 'Diff2'
    end

    # iterate over all tables with info
    info['tables']['mismatching'].each do |table_key, table_value|

      # columns
      table_value['columns']['missing'].each do |column|
        _warn "Column '#{column}' does not exist in schema '#{source_schema}'", 'Diff3'
      end

      table_value['columns']['extra'].each do |column|
        _warn "Column '#{column}' exists in '#{source_schema}' but does not exist in schema '#{target_schema}'", 'Diff4'
      end

      table_value['columns']['mismatching'].each do |column|
        _warn "Column '#{column}' does not match between schemas", 'Diff5' # TODO: fix this message
      end

      # triggers
      table_value['triggers']['missing'].each do |trigger|
        _warn "Trigger '#{trigger}' does not exist in schema '#{source_schema}'", 'Diff3'
      end

      table_value['triggers']['extra'].each do |trigger|
        _warn "Trigger '#{trigger}' exists in '#{source_schema}' but does not exist in schema '#{target_schema}'", 'Diff4'
      end

      table_value['triggers']['mismatching'].each do |trigger|
        _warn "Trigger '#{trigger}' does not match between schemas", 'Diff5' # TODO: fix this message
      end

    end

  end

end