module Nando
  class MigrationTypeError < StandardError
    def initialize (type)
      super
    end
  end
end