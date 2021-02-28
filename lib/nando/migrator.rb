module NandoMigrator
  
  # creates a new migration for the tool
  def self.new_migration (args = {})
    migration_name = args.fetch(:name).underscore
    migration_timestamp = Time.now.strftime("%Y%m%d%H%M%S") # same format as ActiveRecord: year-month-day-hour-minute-second
  
    migration_file_name = "#{migration_timestamp}_#{migration_name}"
  
    dir = 'db/migrate' # this might change later
    path = "#{dir}/#{migration_file_name}"

    create_migration_file(path)

    puts "Creating a new migration: #{migration_file_name}"
  end

  def self.create_migration_file (filepath)
    dir = File.dirname(filepath)

    if !File.directory?(dir)
      STDERR.puts %Q[No directory "#{dir}" was found.]
      exit 3
    end
    
    filepath << ".rb" # append the rb extension
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
