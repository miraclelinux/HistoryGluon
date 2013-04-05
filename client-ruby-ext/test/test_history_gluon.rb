$:.unshift(File.expand_path(File.dirname(__FILE__)) + "/..")
$:.unshift(File.expand_path(File.dirname(__FILE__)) + "/../lib")

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
    actual = hgl.range_query(1, 1, 0, 21, 0, HistoryGluon::SORT_ASCENDING, 100)
    expected = [
                { :id => 1, :type => 2, :sec => 1,  :ns => 0,  :value => 5},
                { :id => 1, :type => 0, :sec => 10, :ns => 10, :value => 5.0},
                { :id => 1, :type => 1, :sec => 20, :ns => 20, :value => "5.0"},
               ]
    assert_equal(expected, actual);
  end
end

class HistoryGluonSortTypeTestCase < Test::Unit::TestCase
  data("ASCENDING"  => [0, HistoryGluon::SORT_ASCENDING],
       "DESCENDING" => [1, HistoryGluon::SORT_DESCENDING],
       "NOT_SORTED" => [2, HistoryGluon::SORT_NOT_SORTED])

  def test_sort_type(data)
    expected, value = data
    assert_equal(expected, value)
  end
end

class HistoryGluonSortTypeExceptionTestCase < Test::Unit::TestCase
  data("ASCENDING"  => HistoryGluon::SORT_ASCENDING,
       "DESCENDING" => HistoryGluon::SORT_DESCENDING)
  def test_valid(sort_type)
    hgl = HistoryGluon.new("zabbix", "localhost", 0)
    assert_nothing_raised do
      hgl.range_query(1, 1, 0, 21, 0, sort_type, 100)
    end
  end

  data("NOT_SORTED" => HistoryGluon::SORT_NOT_SORTED,
       "TOO_LARGE_SORT_TYPE" => 3,
       "TOO_SMALL_SORT_TYPE" => -1)
  def test_invalid(sort_type)
    hgl = HistoryGluon.new("zabbix", "localhost", 0)
    assert_raise("HistoryGluon::SvInvalidSortTypeError") do
      hgl.range_query(1, 1, 0, 21, 0, sort_type, 100)
    end
  end
end
