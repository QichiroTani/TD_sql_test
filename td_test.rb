
require_relative "db_client"

client = DBClient::TD.new

col_ary = %w(usual_greeting_abs)

tag_ary = %w(usual_greeting)

target_ary = %w(true false)
#target_ary = %w(true)

db_name = 'oksky_chat'

jobnum =""
tag_ary.size.times do |i|
  target_ary.size.times do |j|

    sql = <<-EOS
      SELECT 
        EXPLODE(
          ngrams(
            words_ary, 
            3,
            100
          )
        ) as #{col_ary[i]}_#{target_ary[j]}
    FROM
    (
      SELECT
        tokenize_ja(message_content) as words_ary,
        is_customer,
        message_room_id,
        tag_first
      FROM
        analyze_message_and_tag_sets_t
      WHERE
        is_customer = "#{target_ary[j]}"
      ORDER BY
        tag_first
    ) t
    WHERE
      tag_first = "#{tag_ary[i]}"
    EOS
    result_url = "gspreadsheet://tani@solairo.co.jp//analyze_message_tag/#{tag_ary[i]}_#{target_ary[j]}"
    #result_url = "gspreadsheet://tani@solairo.co.jp//analyze_message_tag/sheet1?mode=append" #<-追記モードの仕様を捜索中
    client.query(db_name,sql,result_url)
    jobnum = client.job_num #job番号取得

    #send_event('gspreadsheet://tani@solairo.co.jp/analyze_message_tag/sheet1?mode=append', { items: results })

  end   #2.times END
end   #tag_ary.size.times END
#client.job_export(jobnum)
