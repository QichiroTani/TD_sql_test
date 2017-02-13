require 'td'
require 'td-client'
cln = TreasureData::Client.new('8945/f323db81493d1eee98e4f363e9124df4fbaa0dd8')
cln.databases.each { |db|
  db.tables.each { |tbl|
    p tbl.db_name
    p tbl.table_name
    p tbl.count
  }
}