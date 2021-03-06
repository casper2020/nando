require 'pg'

TestMigration = 100 # TODO: remove this

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

    @db_connection = get_database_connection()
    applied_migrations = get_applied_migrations()

    for filename in migration_files do
      migration_version, migration_name = get_migration_version_and_name(filename)

      if applied_migrations[migration_version]
        next
      end
      puts "Applying: #{filename}"

      classname = get_migration_classname(migration_name)
      puts classname

      # execute <<-'SQL'
      #   INSERT INTO users (name, email) VALUES ('Nandex', 'nandex@mail.com');
      # SQL
      # execute_migration_up();
    end

  end

  # rollbacks 1 migration (or more depending on argument)
  def self.rollback (args = {})
    puts "Rollback!"
  end

  # TODO: might add a migrate:down to distinguish from rollback, similarly to ActiveRecord

  # --------------------------------------------------------

  # TODO: this might get moved, used to execute SQL in the migrations
  def self.execute (sql)
    @db_connection.exec(sql)
  end

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

  def self.get_applied_migrations ()
    # run the query
    results = @db_connection.exec("SELECT * FROM schema_migrations")

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

  def self.get_migration_version_and_name (filename)
    match = /^(\d+)\_(.*)\.rb/.match(filename)
    migration_version = match[1] # by this point, a filename has already been validated, so I don't need to double check
    migration_name = match[2]
    return migration_version, migration_name
  end

  def self.get_migration_classname (filename)
    name = filename.camelize
    Object.const_defined?(name) ? name : Object.const_missing(name) # if the constant does not exist, raise error
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
  # used to convert to snake case (Rails)
  def underscore
    self.gsub(/::/, '/')
        .gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2')
        .gsub(/([a-z\d])([A-Z])/,'\1_\2')
        .tr("-", "_")
        .downcase
  end

  # used to convert to camel or Pascal case (Rails)
  def camelize(uppercase_first_letter = true)
    string = self
    if uppercase_first_letter
      string = string.sub(/^[a-z\d]*/) { |match| match.capitalize }
    else
      string = string.sub(/^(?:(?=\b|[A-Z_])|\w)/) { |match| match.downcase }
    end
    string.gsub(/(?:_|(\/))([a-z\d]*)/) { "#{$1}#{$2.capitalize}" }.gsub("/", "::")
  end
end
