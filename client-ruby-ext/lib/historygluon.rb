require 'historygluon.so'

class HistoryGluon
  SORT_ASCENDING  = 0
  SORT_DESCENDING = 1
  SORT_NOT_SORTED = 2

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
