module NandoSchemaDiff

  SCHEMA_PLACEHOLDER = '___SCHEMANAME___'
  TABLE_TYPE = {
    'r' => :tables,
    'v' => :views
  }

  def self.diff_schemas (source_schema, target_schema)

    @schema_variable = NandoMigrator.instance.schema_variable

    source_info = get_info_base_structure()
    target_info = get_info_base_structure()

    source = get_schema_structure(source_schema)
    target = get_schema_structure(target_schema)

    # start comparing structure

    # checking for different tables
    check_different_tables(source[:tables], target[:tables], source_info, target_info)
    check_different_tables(target[:tables], source[:tables], target_info, source_info)

    # checking for different views
    check_different_views(source[:views], target[:views], source_info, target_info)
    check_different_views(target[:views], source[:views], target_info, source_info)


    # checking for different columns in all shared tables
    check_different_columns(source[:tables], target[:tables], source_info, target_info)
    check_different_columns(target[:tables], source[:tables], target_info, source_info)

    # checking for mismatching columns in all shared tables
    check_mismatching_columns(source[:tables], target[:tables], source_info, target_info)
    check_mismatching_columns(target[:tables], source[:tables], target_info, source_info)


    # checking for different triggers in all shared tables
    check_different_triggers(source[:tables], target[:tables], source_info, target_info)
    check_different_triggers(target[:tables], source[:tables], target_info, source_info)

    # checking for mismatching triggers in all shared tables
    check_mismatching_triggers(source[:tables], target[:tables], source_info, target_info)
    check_mismatching_triggers(target[:tables], source[:tables], target_info, source_info)


    # checking for different constraints in all shared tables
    check_different_constraints(source[:tables], target[:tables], source_info, target_info)
    check_different_constraints(target[:tables], source[:tables], target_info, source_info)

    # checking for mismatching constraints in all shared tables
    check_mismatching_constraints(source[:tables], target[:tables], source_info, target_info)
    check_mismatching_constraints(target[:tables], source[:tables], target_info, source_info)


    # checking for different indexes in all shared tables
    check_different_indexes(source[:tables], target[:tables], source_info, target_info)
    check_different_indexes(target[:tables], source[:tables], target_info, source_info)

    # checking for mismatching indexes in all shared tables
    check_mismatching_indexes(source[:tables], target[:tables], source_info, target_info)
    check_mismatching_indexes(target[:tables], source[:tables], target_info, source_info)


    source_suggestions = print_diff_info(source_info, @schema_variable, source_schema, target_schema)
    target_suggestions = print_diff_info(target_info, @schema_variable, target_schema, source_schema)

    # TODO: might skip this if there is no diff

    wants_suggestions = NandoInterface.get_user_input_boolean("Do want to see the suggestions for changing the schema?")
    if !wants_suggestions
      return
    end

    # suggestions
    puts "\n\n===========================//===========================\n".magenta.bold
    puts "\nSuggestion for ".magenta.bold + "'up'".white.bold + ":".magenta.bold
    print_schema_correction_suggestions(@schema_variable, source_suggestions)

    puts "\nSuggestion for ".magenta.bold + "'down'".white.bold + ":".magenta.bold
    print_schema_correction_suggestions(@schema_variable, target_suggestions)
    puts ""
  end

  def self.get_schema_structure (curr_schema)
    schema_structure = {
      :tables => {},
      :views => {}
    }
    db_connection = NandoMigrator.instance.get_database_connection()

    # get all tables/views in the schema
    results = db_connection.exec("
      SELECT n.nspname AS table_schema,
             t.relname AS table_name,
             t.relkind AS table_type
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
       WHERE t.relkind IN ('r', 'v')
         AND n.nspname = '#{curr_schema}'
    ")

    for row in results do
      schema_structure[TABLE_TYPE[row['table_type']]][row['table_name']] = {
        :columns      => {},
        :triggers     => {},
        :constraints  => {},
        :indexes      => {}
      }
    end

    # get all columns for each table/view
    results = db_connection.exec("
      SELECT n.nspname      AS table_schema,
             t.relname      AS table_name,
             t.relkind      AS table_type,
             a.attname      AS column_name,
             a.atthasdef    AS column_has_default,
             a.attnotnull   AS column_not_null,
             ROW_NUMBER () OVER (PARTITION BY t.oid ORDER BY a.attnum) AS column_num,
             pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_datatype,
             (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                FROM pg_catalog.pg_attrdef d
               WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS column_default
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class t ON a.attrelid = t.oid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
       WHERE a.attnum > 0
         AND NOT a.attisdropped
         AND t.relkind IN ('r', 'v')
         AND n.nspname = '#{curr_schema}'
       ORDER BY column_num
    ")

    for row in results do
      schema_structure[TABLE_TYPE[row['table_type']]][row['table_name']][:columns][row['column_name']] = {
        :column_num           => row['column_num'], # column_num does not use a.attnum, since that field keeps incrementing after dropping/adding columns
        :column_has_default   => row['column_has_default'],
        :column_default       => row['column_default'].nil? ? row['column_default'] : row['column_default'].gsub(curr_schema, SCHEMA_PLACEHOLDER), # remove the schema, since sequences include it in their name
        :column_not_null      => row['column_not_null'],
        :column_datatype      => row['column_datatype']
      }
    end

    # get all triggers for each table
    results = db_connection.exec("
      SELECT n.nspname      AS table_schema,
             t.relname      AS table_name,
             t.relkind      AS table_type,
             tr.tgname      AS trigger_name,
             pg_catalog.pg_get_triggerdef(tr.oid, true) AS trigger_definition
        FROM pg_catalog.pg_trigger tr
        JOIN pg_catalog.pg_class t ON tr.tgrelid = t.oid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
       WHERE t.relkind IN ('r', 'v')
         AND (NOT tr.tgisinternal OR (tr.tgisinternal AND tr.tgenabled = 'D'))
         AND n.nspname = '#{curr_schema}'
    ")

    for row in results do
      schema_structure[TABLE_TYPE[row['table_type']]][row['table_name']][:triggers][row['trigger_name']] = {
        :trigger_definition => row['trigger_definition'].gsub(curr_schema, SCHEMA_PLACEHOLDER) # replace the schema with a value to later replace, to create the trigger definition on the new schema
      }
    end

    # get all constraints for each table
    # TODO: this will get face some issues with VFK to public_companies (that logic is specific to CW, but might make an exception)
    results = db_connection.exec("
      SELECT n.nspname   AS table_schema,
             t.relname   AS table_name,
             t.relkind   AS table_type,
             con.conname AS constraint_name,
             con.consrc  AS constraint_source,
             pg_get_constraintdef(con.oid, true) AS constraint_definition
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class t ON con.conrelid = t.oid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
       WHERE t.relkind IN ('r', 'v')
         AND n.nspname = '#{curr_schema}'
    ")

    for row in results do
      schema_structure[TABLE_TYPE[row['table_type']]][row['table_name']][:constraints][row['constraint_name']] = {
        :constraint_source      => row['constraint_source'],
        :constraint_definition  => row['constraint_definition']
      }
    end

    # get all indexes for each table
    results = db_connection.exec("
      SELECT n.nspname    AS table_schema,
             t.relname    AS table_name,
             t.relkind    AS table_type,
             i.relname    AS index_name,
             pg_catalog.pg_get_indexdef(ix.indexrelid, 0, true) AS index_definition,
             array_to_string(array_agg(a.attname), ', ') AS index_columns
        FROM pg_catalog.pg_index ix
        JOIN pg_catalog.pg_class i ON ix.indexrelid = i.oid
        JOIN pg_catalog.pg_class t ON ix.indrelid = t.oid
        JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
       WHERE t.relkind IN ('r', 'v')
         AND a.attnum = ANY(ix.indkey)
         AND n.nspname = '#{curr_schema}'
       GROUP BY 1, 2, 3, 4, 5
    ")

    for row in results do
      schema_structure[TABLE_TYPE[row['table_type']]][row['table_name']][:indexes][row['index_name']] = {
        :index_definition     => row['index_definition'].gsub(curr_schema, SCHEMA_PLACEHOLDER), # replace the schema with a value to later replace, to create the trigger definition on the new schema
        :index_columns        => row['index_columns']
      }
    end

    return schema_structure
  end

  def self.get_info_base_structure
    return {
      :tables => {
        :missing => {},
        :extra => [],
        :mismatching => {}
      },
      :views => {
        :missing => [],
        :extra => [],
        :mismatching => {}
      }
    }
  end

  def self.setup_table_info (info, table_name)
    # create table structure if one does not exist
    if info[:tables][:mismatching][table_name].nil?
      info[:tables][:mismatching][table_name] = {
        :columns => {
          :missing => {},
          :extra => [],
          :mismatching => {}
        },
        :indexes => {
          :missing => {},
          :extra => [],
          :mismatching => {}
        },
        :triggers => {
          :missing => {},
          :extra => [],
          :mismatching => {}
        },
        :constraints => {
          :missing => {},
          :extra => [],
          :mismatching => {}
        }
      }
    end
  end


  # table comparison
  def self.check_different_tables (left_schema, right_schema, left_info, right_info)
    if !(keys_diff = left_schema.keys - right_schema.keys).empty?
      left_info[:tables][:extra] += keys_diff
    end

    if !(keys_diff = right_schema.keys - left_schema.keys).empty?
      keys_diff.each do |table_key|
        left_info[:tables][:missing][table_key] = right_schema[table_key]
      end
    end
  end

  # views comparison
  def self.check_different_views (left_schema, right_schema, left_info, right_info)
    if !(keys_diff = left_schema.keys - right_schema.keys).empty?
      left_info[:views][:extra] += keys_diff
    end

    if !(keys_diff = right_schema.keys - left_schema.keys).empty?
      left_info[:views][:missing] += keys_diff
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

      if !(keys_diff = left_schema[table_key][:columns].keys - right_schema[table_key][:columns].keys).empty?
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:columns][:extra] += keys_diff
      end

      if !(keys_diff = right_schema[table_key][:columns].keys - left_schema[table_key][:columns].keys).empty?
        setup_table_info(left_info, table_key)
        keys_diff.each do |column_key|
          left_info[:tables][:mismatching][table_key][:columns][:missing][column_key] = right_schema[table_key][:columns][column_key]
        end
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
        if (!left_info[:tables][:mismatching][table_key].nil? && !right_info[:tables][:mismatching][table_key].nil?) && (left_info[:tables][:mismatching][table_key][:columns][:missing].include?(column_key) || right_info[:tables][:mismatching][table_key][:columns][:missing].include?(column_key))
          _debug "Skipping column: #{column_key}"
          next
        end

        if left_schema[table_key][:columns][column_key] != right_schema[table_key][:columns][column_key]
          setup_table_info(left_info, table_key)
          left_info[:tables][:mismatching][table_key][:columns][:mismatching][column_key] = merge_left_right_hashes(left_schema[table_key][:columns][column_key], right_schema[table_key][:columns][column_key])
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

      if !(keys_diff = left_schema[table_key][:triggers].keys - right_schema[table_key][:triggers].keys).empty?
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:triggers][:extra] += keys_diff
      end

      if !(keys_diff = right_schema[table_key][:triggers].keys - left_schema[table_key][:triggers].keys).empty?
        setup_table_info(left_info, table_key)
        keys_diff.each do |trigger_key|
          left_info[:tables][:mismatching][table_key][:triggers][:missing][trigger_key] = right_schema[table_key][:triggers][trigger_key]
        end
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
        if (!left_info[:tables][:mismatching][table_key].nil? && !right_info[:tables][:mismatching][table_key].nil?) && (left_info[:tables][:mismatching][table_key][:triggers][:missing].include?(trigger_key) || right_info[:tables][:mismatching][table_key][:triggers][:missing].include?(trigger_key))
          _debug "Skipping trigger: #{trigger_key}"
          next
        end

        if left_schema[table_key][:triggers][trigger_key] != right_schema[table_key][:triggers][trigger_key]
          setup_table_info(left_info, table_key)
          left_info[:tables][:mismatching][table_key][:triggers][:mismatching][trigger_key] = merge_left_right_hashes(left_schema[table_key][:triggers][trigger_key], right_schema[table_key][:triggers][trigger_key])
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

      if !(keys_diff = left_schema[table_key][:constraints].keys - right_schema[table_key][:constraints].keys).empty?
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:constraints][:extra] += keys_diff
      end

      if !(keys_diff = right_schema[table_key][:constraints].keys - left_schema[table_key][:constraints].keys).empty?
        setup_table_info(left_info, table_key)
        keys_diff.each do |constraint_key|
          left_info[:tables][:mismatching][table_key][:constraints][:missing][constraint_key] = right_schema[table_key][:constraints][constraint_key]
        end
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
        if (!left_info[:tables][:mismatching][table_key].nil? && !right_info[:tables][:mismatching][table_key].nil?) && (left_info[:tables][:mismatching][table_key][:constraints][:missing].include?(constraint_key) || right_info[:tables][:mismatching][table_key][:constraints][:missing].include?(constraint_key))
          _debug "Skipping constraint: #{constraint_key}"
          next
        end

        if left_schema[table_key][:constraints][constraint_key] != right_schema[table_key][:constraints][constraint_key]
          setup_table_info(left_info, table_key)
          left_info[:tables][:mismatching][table_key][:constraints][:mismatching][constraint_key] = merge_left_right_hashes(left_schema[table_key][:constraints][constraint_key], right_schema[table_key][:constraints][constraint_key])
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

      if !(keys_diff = left_schema[table_key][:indexes].keys - right_schema[table_key][:indexes].keys).empty?
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:indexes][:extra] += keys_diff
      end

      if !(keys_diff = right_schema[table_key][:indexes].keys - left_schema[table_key][:indexes].keys).empty?
        setup_table_info(left_info, table_key)
        keys_diff.each do |index_key|
          left_info[:tables][:mismatching][table_key][:indexes][:missing][index_key] = right_schema[table_key][:indexes][index_key]
        end
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
        if (!left_info[:tables][:mismatching][table_key].nil? && !right_info[:tables][:mismatching][table_key].nil?) && (left_info[:tables][:mismatching][table_key][:indexes][:missing].include?(index_key) || right_info[:tables][:mismatching][table_key][:indexes][:missing].include?(index_key))
          _debug "Skipping index: #{index_key}"
          next
        end

        if left_schema[table_key][:indexes][index_key] != right_schema[table_key][:indexes][index_key]
          setup_table_info(left_info, table_key)
          left_info[:tables][:mismatching][table_key][:indexes][:mismatching][index_key] = merge_left_right_hashes(left_schema[table_key][:indexes][index_key], right_schema[table_key][:indexes][index_key])
        end
      end
    end
  end

  def self.print_diff_info (info, suggestion_schema, source_schema, target_schema)
    puts "\nComparing '#{source_schema}' to '#{target_schema}'".magenta.bold

    extra_tables = {}
    missing_tables = {}
    mismatching_tables = {}
    mismatching_views = {}

    info[:tables][:extra].each do |table|
      print_extra "Table '#{table}'"
      extra_tables[table] = "DROP TABLE IF EXISTS #{suggestion_schema}.#{table};"
    end

    info[:tables][:missing].each do |table_key, table_value|
      print_missing "Table '#{table_key}'"
      missing_tables[table_key] = build_create_table_lines(table_key, table_value)
    end

    # iterate over all tables with info
    info[:tables][:mismatching].each do |table_key, table_value|
      print_mismatching "Table '#{table_key}'"

      mismatching_tables[table_key] = {
        :isolated_drop_commands => [],
        :isolated_create_commands => [],
        :alter_tables => [],
        :warnings => []
      }

      # alter tables
      table_value[:constraints][:extra].each do |constraint|
        print_extra "  Constraint '#{constraint}'"
        mismatching_tables[table_key][:alter_tables] << "DROP CONSTRAINT IF EXISTS \"#{constraint}\""
      end

      table_value[:columns][:extra].each do |column|
        print_extra "  Column '#{column}'"
        mismatching_tables[table_key][:alter_tables] << "DROP COLUMN IF EXISTS #{column}"
      end

      table_value[:columns][:missing].each do |column_key, column_value|
        print_missing "  Column '#{column_key}'"
        mismatching_tables[table_key][:alter_tables] << build_add_column_line(column_key, column_value, table_key)
      end

      table_value[:columns][:mismatching].each do |column_key, column_value|
        print_mismatching "  Column '#{column_key}'"
        column_warnings, column_alter_tables = build_mismatching_column_lines(column_key, column_value)
        mismatching_tables[table_key][:warnings] += column_warnings
        mismatching_tables[table_key][:alter_tables] += column_alter_tables
      end

      table_value[:constraints][:missing].each do |constraint_key, constraint_value|
        print_missing "  Constraint '#{constraint_key}'"
        mismatching_tables[table_key][:alter_tables] << build_add_constraint_line(constraint_key, constraint_value)
      end

      # isolated drop commands
      table_value[:triggers][:extra].each do |trigger|
        print_extra "  Trigger '#{trigger}'"
        mismatching_tables[table_key][:isolated_drop_commands] << "DROP TRIGGER IF EXISTS #{trigger} ON #{suggestion_schema}.#{table_key}"
      end

      table_value[:indexes][:extra].each do |index|
        print_extra "  Index '#{index}'"
        mismatching_tables[table_key][:isolated_drop_commands] << "DROP INDEX IF EXISTS #{suggestion_schema}.#{index}"
      end

      # isolated create commands
      table_value[:indexes][:missing].each do |index_key, index_value|
        print_missing "  Index '#{index_key}'"
        mismatching_tables[table_key][:isolated_create_commands] << build_add_index_line(index_key, index_value)
      end

      table_value[:triggers][:missing].each do |trigger_key, trigger_value|
        print_missing "  Trigger '#{trigger_key}'"
        mismatching_tables[table_key][:isolated_create_commands] << build_add_trigger_line(trigger_key, trigger_value)
      end

      # warnings
      table_value[:triggers][:mismatching].each do |trigger_key, trigger_value|
        print_mismatching "  Trigger '#{trigger_key}'"
        mismatching_tables[table_key][:warnings] += build_mismatching_trigger_lines(trigger_key, trigger_value)
      end

      table_value[:constraints][:mismatching].each do |constraint_key, constraint_value|
        print_mismatching "  Constraint '#{constraint_key}'"
        mismatching_tables[table_key][:warnings] += build_mismatching_constraint_lines(constraint_key, constraint_value)
      end

      table_value[:indexes][:mismatching].each do |index_key, index_value|
        print_mismatching "  Index '#{index_key}'"
        mismatching_tables[table_key][:warnings] += build_mismatching_index_lines(index_key, index_value)
      end

    end

    # iterate over all views with info
    info[:views][:extra].each do |view_key|
      print_extra "View '#{view_key}'"
      mismatching_views[view_key] = "View '#{view_key.bold}' exists in '#{source_schema}' but not in the target schema. Might need to drop it"
    end

    info[:views][:missing].each do |view_key|
      print_missing "View '#{view_key}'"
      mismatching_views[view_key] = "View '#{view_key.bold}' does not exist in '#{source_schema}'. Might need to recreate it"
    end

    info[:views][:mismatching].each do |view_key|
      print_mismatching "View '#{view_key}'"
      mismatching_views[view_key] = "View '#{view_key.bold}' does not match between schemas, please recreate it"
    end

    command_suggestions = {
      :extra_tables => extra_tables,
      :missing_tables => missing_tables,
      :mismatching_tables => mismatching_tables,
      :mismatching_views => mismatching_views
    }

    return command_suggestions
  end

  def self.print_schema_correction_suggestions (schema_suggestion, suggestions)

    suggestions[:extra_tables].each do |table_key, command|
      puts "\n-- #{table_key}".white.bold
      puts "#{command}".green.bold
    end

    suggestions[:missing_tables].each do |table_key, table_value|
      puts "\n-- #{table_key}".white.bold
      puts "CREATE TABLE IF NOT EXISTS #{schema_suggestion}.#{table_key}();".green.bold
      print_alter_table_commands(schema_suggestion, table_key, table_value[:alter_tables])
      table_value[:isolated_commands].each do |command|
        puts "#{command};".green.bold
      end
      table_value[:warnings].each do |warning|
        _warn "#{warning}"
      end
    end

    suggestions[:mismatching_tables].each do |table_key, table_value|
      puts "\n-- #{table_key}".white.bold

      # print isolated drop commands
      table_value[:isolated_drop_commands].each do |command|
        puts "#{command};".green.bold
      end

      print_alter_table_commands(schema_suggestion, table_key, table_value[:alter_tables])

      # print isolated create commands
      table_value[:isolated_create_commands].each do |command|
        puts "#{command};".green.bold
      end

      # print warnings
      table_value[:warnings].each do |command|
        _warn "#{command}"
      end
    end

    suggestions[:mismatching_views].each do |view_key, command|
      puts "\n-- #{view_key} (View)".white.bold
      _warn "#{command}"
    end
  end

  def self.print_extra (message)
    puts "+ #{message}".green.bold
  end

  def self.print_missing (message)
    puts "- #{message}".red.bold
  end

  def self.print_mismatching (message)
    puts "? #{message}".yellow.bold
  end

  # print all alter table commands together
  def self.print_alter_table_commands (schema, table_key, commands)
    if commands.length > 0
      puts "ALTER TABLE #{schema}.#{table_key}".green.bold
      commands.each_with_index do |command, index|
        terminator = (index == commands.length - 1) ? ';' : ',';
        puts "  #{command}#{terminator}".green.bold
      end
    end
  end

  # takes 2 hashes and returns a object with both of the keys remapped
  def self.merge_left_right_hashes (left_hash, right_hash)
    left_rehashed = remap_hash(left_hash, 'left_')
    right_rehashed = remap_hash(right_hash, 'right_')
    merged_hash = {}.merge(left_rehashed).merge(right_rehashed)
    return merged_hash
  end

  def self.remap_hash (hash, prefix)
    return_hash = {}
    hash.each do |key, value|
      new_symbol = prefix + key.to_s
      return_hash[new_symbol.to_sym] = value
    end
    return return_hash
  end

  # functions to build certain SQL commands
  def self.build_create_table_lines(table_key, table_value)
    alter_tables = []
    isolated_commands = []
    warnings = []

    # columns
    table_value[:columns].each do |column_key, column_value|
      alter_tables << build_add_column_line(column_key, column_value, table_key)
    end

    # triggers
    table_value[:triggers].each do |trigger_key, trigger_value|
      isolated_commands << build_add_trigger_line(trigger_key, trigger_value)
    end

    # constraints
    table_value[:constraints].each do |constraint_key, constraint_value|
      alter_tables << build_add_constraint_line(constraint_key, constraint_value)
    end

    # indexes
    table_value[:indexes].each do |index_key, index_value|
      isolated_commands << build_add_index_line(index_key, index_value)
    end

    # warnings
    warnings << 'When creating a table, keep in mind the tablespace!'
    warnings << 'This is merely a suggestion of the table structure, it\'s prefered to create the table columns inside the CREATE TABLE command'

    return {
      :alter_tables => alter_tables,
      :isolated_commands => isolated_commands,
      :warnings => warnings
    }
  end

  def self.build_add_column_line (column_key, column_info, table_key)
    # if the column has a default value that matches serials, suggest column as SERIAL
    # in serials, the default value is always like "nextval('<table_name>_<col_name>_seq')"
    if column_info[:column_default] == "nextval('#{SCHEMA_PLACEHOLDER}.#{table_key}_#{column_key}_seq'::regclass)"
      add_column_line = "ADD COLUMN #{column_key} SERIAL"
    else
      data_type = column_info[:column_datatype]
      has_default = column_info[:column_has_default] == 't' ? true : false
      default_string = has_default ? "DEFAULT #{column_info[:column_default]}" : ''
      nullable = column_info[:column_not_null] == 't' ? 'NOT NULL' : ''
      add_column_line = "ADD COLUMN #{column_key} #{data_type} #{nullable} #{default_string}"
    end

    # replace placeholder, clear extra spaces
    return add_column_line.gsub(SCHEMA_PLACEHOLDER, @schema_variable).gsub(/\s+/, ' ').strip
  end

  def self.build_mismatching_column_lines (column_key, column_info)
    warnings = []
    alter_tables = []
    caution_message = " Changing this property may cause problems, use with caution!".light_red # "↳" -> symbol if I decide to pass this warning in a separate line

    if column_info[:left_column_num] != column_info[:right_column_num]
      warnings << "Column '#{column_key}' is on position '#{column_info[:left_column_num]}' on current schema, but on position '#{column_info[:right_column_num]}' in the target schema"
    end
    if column_info[:left_column_default] != column_info[:right_column_default]
      operation = column_info[:right_column_has_default] == 't' ? "SET DEFAULT #{column_info[:right_column_default]}".gsub(SCHEMA_PLACEHOLDER, @schema_variable) : "DROP DEFAULT"
      warnings << "Column '#{column_key.bold}' DEFAULT value differs between schemas." + caution_message
      alter_tables << "ALTER COLUMN #{column_key} #{operation}"
    end
    if column_info[:left_column_datatype] != column_info[:right_column_datatype]
      warnings << "Column '#{column_key.bold}' TYPE differs between schemas." + caution_message
      alter_tables << "ALTER COLUMN #{column_key} SET DATA TYPE #{column_info[:right_column_datatype]}"
    end
    if column_info[:left_column_not_null] != column_info[:right_column_not_null]
      operation = column_info[:left_column_not_null] == 't' ? 'DROP' : 'SET'
      warnings << "Column '#{column_key.bold}' NOT NULL property differs between schemas." + caution_message
      alter_tables << "ALTER COLUMN #{column_key} #{operation} NOT NULL"
    end
    return warnings, alter_tables
  end

  def self.build_add_trigger_line (trigger_key, trigger_info)
    trigger_def = trigger_info[:trigger_definition].gsub(SCHEMA_PLACEHOLDER, @schema_variable)
    return trigger_def
  end

  def self.build_mismatching_trigger_lines (trigger_key, trigger_info)
    warnings = []
    if trigger_info[:left_trigger_definition] != trigger_info[:right_trigger_definition]
      warnings << "Trigger '#{trigger_key.bold}' definition is different between schemas"
    end
    return warnings
  end

  def self.build_add_constraint_line (constraint_key, constraint_info)
    constraint_def = constraint_info[:constraint_definition].gsub(SCHEMA_PLACEHOLDER, @schema_variable)
    return "ADD CONSTRAINT \"#{constraint_key}\" #{constraint_def}"
  end

  def self.build_mismatching_constraint_lines (constraint_key, constraint_info)
    warnings = []
    if constraint_info[:left_constraint_definition] != constraint_info[:right_constraint_definition]
      warnings << "Constraint '#{constraint_key.bold}' definition is different between schemas"
    end
    return warnings
  end

  def self.build_add_index_line(index_key, index_info)
    index_def = index_info[:index_definition].gsub(SCHEMA_PLACEHOLDER, @schema_variable)
    return index_def
  end

  def self.build_mismatching_index_lines(index_key, index_info)
    warnings = []
    if index_info[:left_index_definition] != index_info[:right_index_definition]
      warnings << "Index '#{index_key.bold}' definition is different between schemas"
    end
    if index_info[:left_index_columns] != index_info[:right_index_columns]
      warnings << "Index '#{index_key.bold}' affects different columns between schemas"
    end
    return warnings
  end

end