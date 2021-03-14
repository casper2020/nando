require 'pg'
require "dotenv"

Dotenv.load('.env')

module NandoMigrator

  def self.read_env_file
    @migration_table = ENV['MIGRATION_TABLE_NAME'] || 'schema_migrations'
    @migration_field = ENV['MIGRATION_TABLE_FIELD'] || 'version'
    @migration_dir   = ENV['MIGRATION_DIR'] || 'db/migrate'

    # accepts urls in the same format as dbmate => protocol://username:password@host:port/database_name
    match = /([a-zA-Z]+)\:\/\/(\w+)\:(\w+)\@([\w\.]+)\:(\d+)\/(\w+)/.match(ENV['DATABASE_URL'])

    @db_protocol = match[1]
    @db_username = match[2]
    @db_password = match[3]
    @db_host = match[4]
    @db_port = match[5]
    @db_name = match[6]
  end

  read_env_file()

  # --------------------------------------------------------

  # creates a new migration for the tool
  def self.new_migration (options = {}, args = [])
    migration_name = args[0].underscore
    migration_type = options[:type] || Nando::Migration.name.demodulize # default type is migration with transaction
    migration_timestamp = Time.now.strftime("%Y%m%d%H%M%S") # same format as ActiveRecord: year-month-day-hour-minute-second

    final_migration_type = camelize_migration_type(migration_type)

    migration_file_name = "#{migration_timestamp}_#{migration_name}"
    migration_file_path = "#{@migration_dir}/#{migration_file_name}.rb"

    MigrationGenerator::create_migration_file(migration_file_path, migration_name, final_migration_type)
  end

  # migrates all missing migrations
  def self.migrate (options = {})
    puts "Migrating!"

    migration_files = get_migration_files(@migration_dir)

    if migration_files.length == 0
      STDERR.puts "No migration files were found in \"#{@migration_dir}\"!"
      exit 1
    end

    @db_connection = get_database_connection()
    applied_migrations = get_applied_migrations()

    for filename in migration_files do
      migration_version, migration_name = get_migration_version_and_name(filename)

      if applied_migrations[migration_version]
        next
      end

      execute_migration_method(:up, filename, migration_name, migration_version)
    end

  end

  # rollbacks 1 migration (or more depending on argument)
  def self.rollback (options = {})
    puts "Rollback!"

    rollback_count = 1 # TODO: temporary constant, add option in command interface

    @db_connection = get_database_connection()
    migrations_to_revert = get_migrations_to_revert(rollback_count)

    if migrations_to_revert.length == 0
      STDERR.puts "There are no migrations to revert!"
      exit 1
    end

    # TODO: create function to just get necessary files
    migration_files = get_migration_files_to_rollback(@migration_dir, migrations_to_revert)
    if migration_files.length == 0
      STDERR.puts "No migration files were found in \"#{@migration_dir}\"!"
      exit 1
    end

    for migration_index in 0...migration_files.length do
      filename = migration_files[migration_index]
      migration_version, migration_name = get_migration_version_and_name(filename)

      execute_migration_method(:down, filename, migration_name, migration_version)
    end
  end

  # TODO: might add a migrate:down to distinguish from rollback, similarly to ActiveRecord

  # --------------------------------------------------------

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

  # TODO: might merge with "get_migration_files"
  def self.get_migration_files_to_rollback (directory, versions_to_rollback)
    files = Dir.children(directory)

    migration_files = []
    for filename in files do
      match = /(\d+)\_.*\.rb$/.match(filename)
      if match[0].nil?
        # TODO: test this again for rollback
        puts "Warning: #{filename} does not have a valid migration name"
        next
      end

      if versions_to_rollback.include?(match[1])
        migration_files.push(filename)
      end
    end

    migration_files.sort.reverse # sort and reverse to ensure the migrations are executed chronologically (backwards)
  end

  def self.get_applied_migrations ()
    # run the query
    results = @db_connection.exec("SELECT * FROM #{@migration_table} ORDER BY #{@migration_field} asc")

    applied_migrations = {}
    puts "---------------------------------"
    puts "Applied migrations:"
    results.each{ |row|
      puts "#{row[@migration_field]}"
      applied_migrations[row[@migration_field]] = true
    }
    puts "---------------------------------"
    return applied_migrations
  end

  def self.get_migrations_to_revert (count)
    # run the query
    results = @db_connection.exec("SELECT * FROM #{@migration_table} ORDER BY #{@migration_field} desc LIMIT #{count}")

    migrations_to_rollback = []
    puts "---------------------------------"
    puts "Rollbacked migrations:"
    results.each{ |row|
      puts "#{row[@migration_field]}"
      migrations_to_rollback.push(row[@migration_field])
    }
    puts "---------------------------------"
    return migrations_to_rollback
  end

  def self.execute_migration_method (method, filename, migration_name, migration_version)
    if method == :up
      migrating = true
    else
      migrating = false
    end

    puts migrating ? "Applying: #{filename}" : "Reverting: #{filename}"

    require "./#{@migration_dir}/#{filename}"

    class_const = get_migration_class(migration_name)

    migration_class = class_const.new()
    migration_class.set_connection(@db_connection)
    begin
      migration_class.execute_migration(method)
    rescue => exception
      raise Nando::MigratingError.new(exception)
    end
    update_migration_table(migration_version, migrating)
  end

  def self.update_migration_table (version, to_apply = true)
    if to_apply
      @db_connection.exec("INSERT INTO #{@migration_table} (#{@migration_field}) VALUES (#{version})")
    else
      @db_connection.exec("DELETE FROM #{@migration_table} WHERE #{@migration_field} = '#{version}'")
    end
  end

  def self.camelize_migration_type (migration_type)
    camelize_migration_type = migration_type.camelize
    if !['Migration', 'MigrationWithoutTransaction'].include?(camelize_migration_type)
      raise Nando::MigrationTypeError.new(migration_type) # sending the input value as the error, for easier understanding of the problem for users
    end
    return camelize_migration_type
  end

  def self.get_migration_version_and_name (filename)
    match = /^(\d+)\_(.*)\.rb/.match(filename)
    migration_version = match[1] # by this point, a filename has already been validated, so I don't need to double check
    migration_name = match[2]
    return migration_version, migration_name
  end

  def self.get_migration_class (filename)
    name = filename.camelize
    Object.const_defined?(name) ? Object.const_get(name) : Object.const_missing(name) # if the constant does not exist, raise error
  end

  def self.get_database_connection
    conn = PG::Connection.open(:hostaddr => @db_host,
                               :port => @db_port,
                               :dbname => @db_name,
                               :user=> @db_username)
  end

end


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
  def camelize (uppercase_first_letter = true)
    string = self
    if uppercase_first_letter
      string = string.sub(/^[a-z\d]*/) { |match| match.capitalize }
    else
      string = string.sub(/^(?:(?=\b|[A-Z_])|\w)/) { |match| match.downcase }
    end
    string.gsub(/(?:_|(\/))([a-z\d]*)/) { "#{$1}#{$2.capitalize}" }.gsub("/", "::")
  end

  # gets the class/module name with the previous class/module names
  def demodulize
    self.split('::').last
  end
end
