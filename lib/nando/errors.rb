module Nando
  class MigrationTypeError < StandardError
    def initialize (type)
      super
    end
  end

  class MigratingError < StandardError
    def initialize (error)
      super
    end
  end
end