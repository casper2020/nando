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

end