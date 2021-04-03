require 'erb'

module MigrationGenerator

  # creates the actual migration file
  def self.create_migration_file (filepath, migration_name, migration_type)
    dir = File.dirname(filepath)

    if !File.directory?(dir)
      STDERR.puts %Q[No directory "#{dir}" was found.]
      exit 3
    end

    case migration_type
    when Nando::Migration.name.demodulize
      template_file_name = 'migration'
    when Nando::MigrationWithoutTransaction.name.demodulize
      template_file_name = 'migration_without_transaction'
    end

    migration_class_name = migration_name.camelize()
    file = File.new(filepath, 'w')
    # TODO: check if binding logic is correct, and if pathing changes when it's a gem
    render_to_file(File.join(File.dirname(File.expand_path(__FILE__)), "templates/#{template_file_name}.rb"), binding, file)

    puts "Creating a new migration: #{filepath}"
  end

  # based on the template renderer from the commercial engine
  def self.render_to_file (template_file, context, output_file)
    output_file.write render(template_file, context)
  end

  def self.render(template_file, context)
    renderer = ERB.new(File.read(template_file), nil, nil)
    renderer.result(context)
  end

  def self.create_baseline_file (filepath, migration_name)
    dir = File.dirname(filepath)

    if !File.directory?(dir)
      STDERR.puts %Q[No directory "#{dir}" was found.]
      exit 3
    end

    @db_connection = NandoMigrator.get_database_connection();
    results = @db_connection.exec("
      SELECT n.nspname AS function_schema,
             p.proname AS function_name,
             l.lanname AS function_language,
             CASE WHEN l.lanname = 'internal' THEN p.prosrc ELSE pg_get_functiondef(p.oid) END AS definition,
             pg_get_function_arguments(p.oid) AS function_arguments,
             t.typname AS return_type,
             p.proowner AS p_owner
        FROM pg_proc p
        LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
        LEFT JOIN pg_language l ON p.prolang = l.oid
        LEFT JOIN pg_type t ON t.oid = p.prorettype
       WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
       ORDER BY function_schema, function_name
    ")

    up_method = ''
    number_of_functions = 0
    indent = '    '
    # TODO: try to indent the functions correctly
    for row in results do
      up_method += "\n" + indent + "update_function <<-'SQL'\n"
      up_method += "#{row['definition']}"
      up_method += "\n" + indent + "SQL\n"
      number_of_functions += 1
    end

    new_file = File.new(filepath, 'w')

    # binding
    migration_class_name = migration_name.camelize
    migration_type = Nando::Migration.name.demodulize # TODO: atm all baseline files are create as migrations with transactions, this might change later
    migration_up_code = up_method
    migration_down_code = indent + "# #{number_of_functions} functions have been added to this baseline"

    render_to_file(File.join(File.dirname(File.expand_path(__FILE__)), 'baseline_templates/migration.rb'), binding, new_file)

    puts "Creating a new baseline: #{filepath}"
  end

end