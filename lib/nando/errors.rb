module Nando
  class GenericError < StandardError
    def initialize (error)
      super
    end
  end

  class InputError < StandardError
    def initialize (error)
      super
    end
  end
end