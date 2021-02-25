require "nando/version"

module Nando
  class Error < StandardError; end
  
  def self.new_migration
    puts 'Creating a new migration'
  end

end
