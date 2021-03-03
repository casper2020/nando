require 'pg'

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

    migration_files = get_migration_files(migration_dir)

    if migration_files.length == 0
      STDERR.puts "No migration files were found in \"#{migration_dir}\"!"
      exit 1
    end

    db_connection = get_database_connection()
    applied_migrations = get_applied_migrations(db_connection)

    for filename in migration_files do
      version = get_migration_version(filename)

      if applied_migrations[version]
        next
      end
      puts "Applying: #{filename}"

      # execute_migration_up();
    end

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

  def self.get_migration_files (directory)
    files = Dir.children(directory)

    migration_files = []
    for filename in files do
      if !/\d\_.*\.rb$/.match(filename)
        puts "Warning: #{filename} does not have a valid migration name"
        next
      end

      migration_files.push(filename)
    end

    migration_files.sort! # sort to ensure the migrations are executed chronologically
  end

  def self.get_applied_migrations (db_connection)

    # run the query
    results = db_connection.exec("SELECT * FROM schema_migrations")

    applied_migrations = {}
    puts "---------------------------------"
    puts "Applied migrations:"
    results.each{ |row|
      puts "#{row["version"]}"
      applied_migrations[row["version"]] = true
    }
    puts "---------------------------------"
    return applied_migrations
  end

  def self.get_migration_version (filename)
    /^(\d+)/.match(filename)[1] # by this point, a filename has already been validated, so I don't need to double check
  end

  def self.get_database_connection
    # TODO: redo this to use dynamic parameters from a .env file
    conn = PGconn.connect( :hostaddr=>"127.0.0.1", :port=>5432, :dbname=>"tests_diss", :user=>"toconline")
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
