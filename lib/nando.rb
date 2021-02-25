require "nando/version"

module Nando
  class Error < StandardError; end
  
  def nando
    puts 'Nando says hi'
  end

  def migrate
    puts 'Gonna migrate'
  end
end
