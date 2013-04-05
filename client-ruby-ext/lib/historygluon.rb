require 'historygluon.so'

class HistoryGluon
  class Error < ::StandardError
    @code = 0

    def self.code
      @code
    end

    def code
      self.class.code
    end
  end

  class SortTypeError < Error
    @code = 202
  end
end
