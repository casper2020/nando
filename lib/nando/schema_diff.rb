module NandoSchemaDiff

  SCHEMA_PLACEHOLDER = '___SCHEMANAME___'
  TABLE_TYPE = {
    'r' => :tables,
    'v' => :views
  }

  def self.diff_schemas (source_schema, target_schema)

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


    # TODO: what to do about views, types, etc

    print_diff_info(source_info, source_schema, target_schema)
    print_diff_info(target_info, target_schema, source_schema)
    puts ""
  end

  def self.get_schema_structure (curr_schema)
    schema_structure = {
      :tables => {},
      :views => {}
    }
    db_connection = NandoMigrator.get_database_connection()

    # TODO: reduce these SELECT * to specific columns

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
             a.attnum       AS column_num,
             a.atthasdef    AS column_has_default,
             a.attnotnull   AS column_not_null,
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
    ")

    for row in results do
      schema_structure[TABLE_TYPE[row['table_type']]][row['table_name']][:columns][row['column_name']] = {
        :column_num           => row['column_num'],
        :column_has_default   => row['column_has_default'],
        :column_default       => row['column_default'].nil? ? row['column_default'] : row['column_default'].gsub(curr_schema, ''), # remove the schema, since sequences include it in their name
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
        :missing => [],
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

  # views comparison
  # TODO: might merge with above, if they stay similiar
  def self.check_different_views (left_schema, right_schema, left_info, right_info)
    if keys_diff = left_schema.keys - right_schema.keys
      left_info[:views][:extra] += keys_diff
    end

    if keys_diff = right_schema.keys - left_schema.keys
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

      if keys_diff = left_schema[table_key][:columns].keys - right_schema[table_key][:columns].keys
        setup_table_info(left_info, table_key)
        left_info[:tables][:mismatching][table_key][:columns][:extra] += keys_diff
      end

      if keys_diff = right_schema[table_key][:columns].keys - left_schema[table_key][:columns].keys
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


  def self.schema_correction_suggestion (source_schema, target_schema, drop_tables, create_tables, mismatching_tables)
    puts "\nSuggestion to turn '#{source_schema}' into '#{target_schema}'".magenta.bold

    # TODO: will remove colors in the end, just here to list commands that are "ready"

    drop_tables.each do |command|
      puts "\n#{command}".green.bold
    end

    create_tables.each do |command|
      puts "\n#{command}"
    end

    mismatching_tables.each do |table_key, table_value|
      # print all isolated commands
      table_value[:isolated_commands].each do |command|
        puts "#{command};".green.bold
      end

      # print all alter table commands necessary
      if table_value[:alter_tables].length > 0
        puts "ALTER TABLE #{source_schema}.#{table_key}".green.bold
        table_value[:alter_tables].each_with_index do |command, index|
          terminator = (index == table_value[:alter_tables].length - 1) ? ';' : ',';
          puts "  #{command}#{terminator}".green.bold
        end
      end

      # print mismatching commands
      table_value[:mismatching].each do |command|
        puts "#{command};".yellow.bold
      end
    end
  end

  def self.print_diff_info (info, source_schema, target_schema)
    puts "\nComparing '#{source_schema}' to '#{target_schema}'".magenta.bold

    drop_tables = []
    create_tables = []
    mismatching_tables = {}

    info[:tables][:extra].each do |table|
      print_extra "Table '#{table}'"
      drop_tables << "DROP TABLE IF EXISTS #{source_schema}.#{table};"
    end

    info[:tables][:missing].each do |table|
      print_missing "Table '#{table}'"
      create_tables << "TODO: MISSING TABLE"
    end

    # iterate over all tables with info
    info[:tables][:mismatching].each do |table_key, table_value|
      print_mismatching "Table '#{table_key}'"

      mismatching_tables[table_key] = {
        :isolated_commands => [],
        :alter_tables => [],
        :mismatching => []
      }

      # columns
      table_value[:columns][:extra].each do |column|
        print_extra "  Column '#{column}'"
        mismatching_tables[table_key][:alter_tables] << "DROP COLUMN IF EXISTS #{column}"
      end

      table_value[:columns][:missing].each do |column_key, column_value|
        print_missing "  Column '#{column_key}'"
        mismatching_tables[table_key][:alter_tables] << build_add_column_line(column_key, column_value)
      end

      table_value[:columns][:mismatching].each do |column|
        print_mismatching "  Column '#{column}'"
        mismatching_tables[table_key][:mismatching] << "TODO: MISMATCHING COLUMN '#{column}'"
      end

      # triggers
      table_value[:triggers][:extra].each do |trigger|
        print_extra "  Trigger '#{trigger}'"
        mismatching_tables[table_key][:isolated_commands] << "DROP TRIGGER IF EXISTS #{trigger} ON #{source_schema}.#{table_key}"
      end

      table_value[:triggers][:missing].each do |trigger|
        print_missing "  Trigger '#{trigger}'"
        mismatching_tables[table_key][:mismatching] << "TODO: MISSING TRIGGER '#{trigger}'"
      end

      table_value[:triggers][:mismatching].each do |trigger|
        print_mismatching "  Trigger '#{trigger}'"
        mismatching_tables[table_key][:mismatching] << "TODO: MISMATCHING TRIGGER '#{trigger}'"
      end

      # constraints
      table_value[:constraints][:extra].each do |constraint|
        print_extra "  Constraint '#{constraint}'"
        mismatching_tables[table_key][:alter_tables] << "DROP CONSTRAINT IF EXISTS '#{constraint}'"
      end

      table_value[:constraints][:missing].each do |constraint|
        print_missing "  Constraint '#{constraint}'"
        mismatching_tables[table_key][:mismatching] << "TODO: MISSING CONSTRAINT '#{constraint}'"
      end

      table_value[:constraints][:mismatching].each do |constraint|
        print_mismatching "  Constraint '#{constraint}'"
        mismatching_tables[table_key][:mismatching] << "TODO: MISMATCHING CONSTRAINT '#{constraint}'"
      end

      # indexes
      table_value[:indexes][:extra].each do |index|
        print_extra "  Index '#{index}'"
        mismatching_tables[table_key][:isolated_commands] << "DROP INDEX IF EXISTS #{source_schema}.#{index}"
      end

      table_value[:indexes][:missing].each do |index|
        print_missing "  Index '#{index}'"
        mismatching_tables[table_key][:mismatching] << "TODO: MISSING INDEX '#{index}'"
      end

      table_value[:indexes][:mismatching].each do |index|
        print_mismatching "  Index '#{index}'"
        mismatching_tables[table_key][:mismatching] << "TODO: MISMATCHING INDEX '#{index}'"
      end

    end

    info[:views][:extra].each do |view|
      print_extra "View '#{view}'"
    end

    info[:views][:missing].each do |view|
      print_missing "View '#{view}'"
    end

    info[:views][:mismatching].each do |view|
      print_mismatching "View '#{view}'"
    end

    # suggestions
    schema_correction_suggestion(source_schema, target_schema, drop_tables, create_tables, mismatching_tables)
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

  # functions to build certain SQL commands
  def self.build_add_column_line (column_key, column_info)
    data_type = column_info[:column_datatype]
    has_default = column_info[:column_has_default] == 't' ? true : false
    default_string = has_default ? "DEFAULT #{column_info[:column_default]}" : ''
    nullable = column_info[:column_not_null] == 't' ? 'NOT NULL' : ''
    return "ADD COLUMN '#{column_key}' #{data_type} #{nullable} #{default_string}".gsub(/\s+/, ' ').strip # build string, clear extra spaces
  end

end