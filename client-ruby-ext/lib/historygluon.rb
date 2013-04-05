require 'historygluon.so'

class HistoryGluon
  class Error < ::StandardError
    def self.code
      @@code
    end

    def code
      self.class.code
    end
  end
end
