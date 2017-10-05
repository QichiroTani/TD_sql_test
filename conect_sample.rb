require 'td'
require 'td-client'
cln = TreasureData::Client.new('********************')
cln.databases.each { |db|
  db.tables.each { |tbl|
    p tbl.db_name
    p tbl.table_name
    p tbl.count
  }
}
