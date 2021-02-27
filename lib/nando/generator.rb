module NandoMigration
  
  # creates a new migration for the tool
  def self.new_migration (args = {})
    migration_name = args.fetch(:name).underscore
    migration_timestamp = Time.now.strftime("%Y%m%d%H%M%S") # same format as ActiveRecord: year-month-day-hour-minute-second
  
    migration_file_name = "#{migration_timestamp}_#{migration_name}"
  
    puts "Creating a new migration: #{migration_file_name}"
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
