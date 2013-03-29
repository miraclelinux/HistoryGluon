require 'historygluon.so'

class HistoryGluon
  class Exception < ::StandardError
    @code = 0

    def self.code
      @code
    end

    def code
      self.class.code
    end
  end

  class SortTypeException < Exception
    @code = 202
  end
end
