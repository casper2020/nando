require 'erb'

module MigrationGenerator

  # creates the actual migration file
  def self.create_migration_file (filepath, migration_name)
    dir = File.dirname(filepath)

    if !File.directory?(dir)
      STDERR.puts %Q[No directory "#{dir}" was found.]
      exit 3
    end

    migration_class_name = migration_name.camelize()
    file = File.new(filepath, 'w')
    # TODO: distinguish between Migration and MigrationWithoutTransaction
    # TODO: check if binding logic is correct, and if pathing changes when it's a gem
    render_to_file('lib/nando/templates/migration.rb', binding, file)

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

end