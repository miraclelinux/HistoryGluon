$:.unshift(File.expand_path(File.dirname(__FILE__)) + "/..")

require 'rubygems'
require 'test-unit'
require 'historygluon'

class HistoryGluonTestCase < Test::Unit::TestCase
  def test_new
    hgl = HistoryGluon.new("zabbix", "localhost", 0)
    assert_equal(hgl.class, HistoryGluon)
  end
end
