def self.new_migration (args = {})
  migration_name = args.fetch(:name).underscore

  puts "Creating a new migration: #{migration_name}"
end

# timestamp example: 20200713100600

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
