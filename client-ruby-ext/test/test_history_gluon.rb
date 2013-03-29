$:.unshift(File.expand_path(File.dirname(__FILE__)) + "/..")

require 'rubygems'
require 'test-unit'
require 'historygluon'

class HistoryGluonTestCase < Test::Unit::TestCase
  def test_new
    hgl = HistoryGluon.new("zabbix", "localhost", 0)
    assert_equal(hgl.class, HistoryGluon)
  end

  def test_range_query
    hgl = HistoryGluon.new("zabbix", "localhost", 0)
    hgl.add_uint(1, 0, 0, 5)
    hgl.add_uint(1, 21, 1, 5)
    hgl.add_uint(1, 1, 0, 5)
    hgl.add_float(1, 10, 10, 5.0)
    hgl.add_string(1, 20, 20, "5.0")
    actual = hgl.range_query(1, 1, 0, 21, 0, 0, 100)
    expected = [
                { :id => 1, :type => 2, :sec => 1,  :ns => 0,  :value => 5},
                { :id => 1, :type => 0, :sec => 10, :ns => 10, :value => 5.0},
                { :id => 1, :type => 1, :sec => 20, :ns => 20, :value => "5.0"},
               ]
    assert_equal(expected, actual);
  end
end
