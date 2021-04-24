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
    check_mismatching_triggers(source, target, source_info, target_info)
    check_mismatching_triggers(target, source, target_info, source_info)


    # checking for different constraints in all shared tables
    check_different_constraints(source, target, source_info, target_info)
    check_different_constraints(target, source, target_info, source_info)

    # checking for mismatching constraints in all shared tables
    check_mismatching_constraints(source, target, source_info, target_info)
    check_mismatching_constraints(target, source, target_info, source_info)


    # checking for different indexes in all shared tables
    check_different_indexes(source, target, source_info, target_info)
    check_different_indexes(target, source, target_info, source_info)

    # checking for mismatching indexes in all shared tables
    check_mismatching_indexes(source, target, source_info, target_info)
    check_mismatching_indexes(target, source, target_info, source_info)


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
        :columns      => {},
        :triggers     => {},
        :constraints  => {},
        :indexes      => {}
      }
    end

    # get all columns for each table
    results = db_connection.exec("
      SELECT *
        FROM information_schema.columns
       WHERE table_schema = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['table_name']][:columns][row['column_name']] = {
        :ordinal_position   => row['ordinal_position'],
        :column_default     => row['column_default'].nil? ? row['column_default'] : row['column_default'].gsub(curr_schema, ''), # remove the schema, since sequences include it in their name
        :is_nullable        => row['is_nullable'],
        :data_type          => row['data_type']
      }
    end

    # get all triggers for each table
    results = db_connection.exec("
      SELECT *
        FROM information_schema.triggers
       WHERE event_object_schema = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['event_object_table']][:triggers][row['trigger_name']] = {
        :event_manipulation   => row['event_manipulation'],
        :action_order         => row['action_order'],
        :action_condition     => row['action_condition'],
        :action_statement     => row['action_statement'],
        :action_orientation   => row['action_orientation'],
        :action_timing        => row['action_timing']
      }
    end

    # get all constraints for each table
    results = db_connection.exec("
      SELECT rel.relname AS table_name,
             con.conname AS constraint_name,
             con.consrc  AS constraint_source
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
        JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
       WHERE nsp.nspname = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['table_name']][:constraints][row['constraint_name']] = {
        :constraint_source   => row['constraint_source']
      }
    end

    # get all indexes for each table
    results = db_connection.exec("
      SELECT *
        FROM pg_catalog.pg_indexes
       WHERE schemaname = '#{curr_schema}';
    ")

    for row in results do
      schema_structure[row['tablename']][:indexes][row['indexname']] = {
        :tablespace     => row['tablespace'],
        :indexdef       => row['indexdef'].gsub(curr_schema, ''), # remove the schema, since indexes include it in their definition
      }
    end

    return schema_structure
  end

  def self.get_info_base_structure
    return {
      :tables => {
        :missing => [],
        :extra => [],
        :mismatching => {}
      },
      :views => {
        # not currently used, but reserved for now
      }
    }
  end

  def self.setup_table_info (info, table_name)
    # create table structure if one does not exist
    if info[:tables][:mismatching][table_name].nil?
      info[:tables][:mismatching][table_name] = {
        :columns => {
          :missing => [],
          :extra => [],
          :mismatching => [] # TODO: replace with hash
        },
        :indexes => {
          :missing => [],
          :extra => [],
          :mismatching => [] # TODO: replace with hash
        },
        :triggers => {
          :missing => [],
          :extra => [],
          :mismatching => [] # TODO: replace with hash
        },
        :constraints => {
          :missing => [],
          :extra => [],
          :mismatching => [] # TODO: replace with hash
        }
      }
    end
  end


  # table comparison
  def self.check_different_tables (left_schema, right_schema, left_info, right_info)
    if keys_diff = left_schema.keys - right_schema.keys
      left_info[:tables][:extra] += keys_diff
    end

    if keys_diff = right_schema.keys - left_schema.keys
      left_info[:tables][:missing] += keys_diff
    end
  end


  # column comparison
  def self.check_different_columns (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info[:tables][:missing].include?(table_key) || right_info[:tables][:missing].include?(table_key)
        _debug "Skipping table (1): #{table_key}"
        next
      end

      if keys_diff = left_schema[table_key][:columns].keys - right_schema[table_key][:columns].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:columns][:extra] += keys_diff
      end

      if keys_diff = right_schema[table_key][:columns].keys - left_schema[table_key][:columns].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:columns][:missing] += keys_diff
      end
    end
  end

  def self.check_mismatching_columns (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info[:tables][:missing].include?(table_key) || right_info[:tables][:missing].include?(table_key)
        _debug "Skipping table (2): #{table_key}"
        next
      end

      table_value[:columns].each do |column_key, column_value|
        # ignore columns that only appear in one of the tables
        if left_info[:tables][:mismatching][table_key][:columns][:missing].include?(column_key) || right_info[:tables][:mismatching][table_key][:columns][:missing].include?(column_key)
          _debug "Skipping column: #{column_key}"
          next
        end

        if left_schema[table_key][:columns][column_key] != right_schema[table_key][:columns][column_key]
          setup_table_info(left_info, table_key)
          left_info[:tables][:mismatching][table_key][:columns][:mismatching] << column_key # TODO: add more info, not just a key
        end
      end
    end
  end


  # trigger comparison
  def self.check_different_triggers (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info[:tables][:missing].include?(table_key) || right_info[:tables][:missing].include?(table_key)
        _debug "Skipping table (3): #{table_key}"
        next
      end

      if keys_diff = left_schema[table_key][:triggers].keys - right_schema[table_key][:triggers].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:triggers][:extra] += keys_diff
      end

      if keys_diff = right_schema[table_key][:triggers].keys - left_schema[table_key][:triggers].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:triggers][:missing] += keys_diff
      end
    end
  end

  def self.check_mismatching_triggers (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info[:tables][:missing].include?(table_key) || right_info[:tables][:missing].include?(table_key)
        _debug "Skipping table (4): #{table_key}"
        next
      end

      table_value[:triggers].each do |trigger_key, trigger_value|
        # ignore triggers that only appear in one of the tables
        if left_info[:tables][:mismatching][table_key][:triggers][:missing].include?(trigger_key) || right_info[:tables][:mismatching][table_key][:triggers][:missing].include?(trigger_key)
          _debug "Skipping trigger: #{trigger_key}"
          next
        end

        if left_schema[table_key][:triggers][trigger_key] != right_schema[table_key][:triggers][trigger_key]
          setup_table_info(left_info, table_key)
          left_info[:tables][:mismatching][table_key][:triggers][:mismatching] << trigger_key # TODO: add more info, not just a key
        end
      end
    end
  end


  # constraint comparison
  def self.check_different_constraints (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info[:tables][:missing].include?(table_key) || right_info[:tables][:missing].include?(table_key)
        _debug "Skipping table (5): #{table_key}"
        next
      end

      if keys_diff = left_schema[table_key][:constraints].keys - right_schema[table_key][:constraints].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:constraints][:extra] += keys_diff
      end

      if keys_diff = right_schema[table_key][:constraints].keys - left_schema[table_key][:constraints].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:constraints][:missing] += keys_diff
      end
    end
  end

  def self.check_mismatching_constraints (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info[:tables][:missing].include?(table_key) || right_info[:tables][:missing].include?(table_key)
        _debug "Skipping table (6): #{table_key}"
        next
      end

      table_value[:constraints].each do |constraint_key, constraint_value|
        # ignore constraints that only appear in one of the tables
        if left_info[:tables][:mismatching][table_key][:constraints][:missing].include?(constraint_key) || right_info[:tables][:mismatching][table_key][:constraints][:missing].include?(constraint_key)
          _debug "Skipping constraint: #{constraint_key}"
          next
        end

        if left_schema[table_key][:constraints][constraint_key] != right_schema[table_key][:constraints][constraint_key]
          setup_table_info(left_info, table_key)
          left_info[:tables][:mismatching][table_key][:constraints][:mismatching] << constraint_key # TODO: add more info, not just a key
        end
      end
    end
  end


  # index comparison
  def self.check_different_indexes (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info[:tables][:missing].include?(table_key) || right_info[:tables][:missing].include?(table_key)
        _debug "Skipping table (7): #{table_key}"
        next
      end

      if keys_diff = left_schema[table_key][:indexes].keys - right_schema[table_key][:indexes].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:indexes][:extra] += keys_diff
      end

      if keys_diff = right_schema[table_key][:indexes].keys - left_schema[table_key][:indexes].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:indexes][:missing] += keys_diff
      end
    end
  end

  def self.check_mismatching_indexes (left_schema, right_schema, left_info, right_info)
    left_schema.each do |table_key, table_value|
      # ignore tables that only appear in one of the schemas
      if left_info[:tables][:missing].include?(table_key) || right_info[:tables][:missing].include?(table_key)
        _debug "Skipping table (8): #{table_key}"
        next
      end

      table_value[:indexes].each do |index_key, index_value|
        # ignore indexes that only appear in one of the tables
        if left_info[:tables][:mismatching][table_key][:indexes][:missing].include?(index_key) || right_info[:tables][:mismatching][table_key][:indexes][:missing].include?(index_key)
          _debug "Skipping index: #{index_key}"
          next
        end

        if left_schema[table_key][:indexes][index_key] != right_schema[table_key][:indexes][index_key]
          setup_table_info(left_info, table_key)
          left_info[:tables][:mismatching][table_key][:indexes][:mismatching] << index_key # TODO: add more info, not just a key
        end
      end
    end
  end


  def self.print_diff_info (info, source_schema, target_schema)
    _warn "Printing '#{source_schema}'"

    info[:tables][:missing].each do |table|
      _warn "Table '#{table}' does not exist in schema '#{source_schema}'", 'Diff1'
    end

    info[:tables][:extra].each do |table|
      _warn "Table '#{table}' exists in '#{source_schema}' but does not exist in schema '#{target_schema}'", 'Diff2'
    end

    # iterate over all tables with info
    info[:tables][:mismatching].each do |table_key, table_value|

      # columns
      table_value[:columns][:missing].each do |column|
        _warn "Column '#{column}' does not exist in schema '#{source_schema}'", 'Diff3'
      end

      table_value[:columns][:extra].each do |column|
        _warn "Column '#{column}' exists in '#{source_schema}' but does not exist in schema '#{target_schema}'", 'Diff4'
      end

      table_value[:columns][:mismatching].each do |column|
        _warn "Column '#{column}' does not match between schemas", 'Diff5' # TODO: fix this message
      end

      # triggers
      table_value[:triggers][:missing].each do |trigger|
        _warn "Trigger '#{trigger}' does not exist in schema '#{source_schema}'", 'Diff6'
      end

      table_value[:triggers][:extra].each do |trigger|
        _warn "Trigger '#{trigger}' exists in '#{source_schema}' but does not exist in schema '#{target_schema}'", 'Diff7'
      end

      table_value[:triggers][:mismatching].each do |trigger|
        _warn "Trigger '#{trigger}' does not match between schemas", 'Diff8' # TODO: fix this message
      end

      # constraints
      table_value[:constraints][:missing].each do |constraint|
        _warn "Constraint '#{constraint}' does not exist in schema '#{source_schema}'", 'Diff9'
      end

      table_value[:constraints][:extra].each do |constraint|
        _warn "Constraint '#{constraint}' exists in '#{source_schema}' but does not exist in schema '#{target_schema}'", 'Diff10'
      end

      table_value[:constraints][:mismatching].each do |constraint|
        _warn "Constraint '#{constraint}' does not match between schemas", 'Diff11' # TODO: fix this message
      end

      # indexes
      table_value[:indexes][:missing].each do |index|
        _warn "Index '#{index}' does not exist in schema '#{source_schema}'", 'Diff12'
      end

      table_value[:indexes][:extra].each do |index|
        _warn "Index '#{index}' exists in '#{source_schema}' but does not exist in schema '#{target_schema}'", 'Diff13'
      end

      table_value[:indexes][:mismatching].each do |index|
        _warn "Index '#{index}' does not match between schemas", 'Diff14' # TODO: fix this message
      end

    end

  end

end