require 'historygluon.so'

class HistoryGluon
  SORT_ASCENDING  = 0
  SORT_DESCENDING = 1
  SORT_NOT_SORTED = 2

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
