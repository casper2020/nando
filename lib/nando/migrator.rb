module NandoMigrator

  @@migration_dir = 'db/migrate' # TODO: might change later to env file

  def self.migration_dir; @@migration_dir end

  # --------------------------------------------------------

  # creates a new migration for the tool
  def self.new_migration (args = {})
    migration_name = args.fetch(:name).underscore
    migration_timestamp = Time.now.strftime("%Y%m%d%H%M%S") # same format as ActiveRecord: year-month-day-hour-minute-second

    migration_file_name = "#{migration_timestamp}_#{migration_name}"
    migration_file_path = "#{migration_dir}/#{migration_file_name}.rb"

    create_migration_file(migration_file_path)
    puts "Creating a new migration: #{migration_file_path}"
  end

  # migrates all missing migrations
  def self.migrate (args = {})
    puts "Migrating!"
  end

  # rollbacks 1 migration (or more depending on argument)
  def self.rollback (args = {})
    puts "Rollback!"
  end

  # TODO: might add a migrate:down to distinguish from rollback, similarly to ActiveRecord

  # --------------------------------------------------------

  def self.create_migration_file (filepath)
    dir = File.dirname(filepath)

    if !File.directory?(dir)
      STDERR.puts %Q[No directory "#{dir}" was found.]
      exit 3
    end

    File.new(filepath, 'w')
  end
end


# module Nando
#   class Error < StandardError; end
# end

class String
  # used to convert to snake case
  def underscore
    self.gsub(/::/, '/')
        .gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
        .gsub(/([a-z\d])([A-Z])/,'\1_\2')
        .tr("-", "_")
        .downcase
  end
end
