
require_relative "db_client"

client = DBClient::TD.new

col_ary = %w(
            usual_greeting_abs
            gratitude_ans
            request_product_search_ladies_set_ans
            request_product_search_taxon_set
            request_product_search_brand_set
            request_product_search_mens_set
            question_service_functionality_chat_ans
            request_product_search_size_set_ans
            check_product_own_ans
            request_product_search_gift_set_ans
            own_product_genre_ans
            request_product_search_color_set_ans
            praise_ans
            request_product_search_design_set_ans
            request_cordinate_search_color_set_ans
            temperature_cold_ans
            question_service_ans
            request_cordinate_search_ans
            question_product_search_ans
            request_product_search_kids_set_ans
            request_cordinate_search_product_set_ans
            question_service_env_ans
            request_product_search_relation_ans
            explanation_product_ans
            question_service_functionality_app_ans
            request_product_search_event_ans
            self_introduction_ans
            request_product_search_ans
            explanation_product_trend_ans
            request_product_ans
            approval_ans
            request_product_search_seasonal_set_ans
            null
)

tag_ary = %w(
            'usual_greeting'
            'gratitude'
            'request_product_search_ladies-set'
            'request_product_search_taxon-set'
            'request_product_search_brand-set'
            'request_product_search_mens-set'
            'question_service_functionality_chat'
            'request_product_search_size-set'
            'check_product_own'
            'request_product_search_gift-set'
            'own_product_genre'
            'request_product_search_color-set'
            'praise'
            'request_product_search_design-set'
            'request_cordinate_search_color-set'
            'temperature_cold'
            'question_service'
            'request_cordinate_search'
            'question_product_search'
            'request_product_search_kids-set'
            'request_cordinate_search_product-set'
            'question_service_env'
            'request_product_search_relation'
            'explanation_product'
            'question_service_functionality_app'
            'request_product_search_event'
            'self_introduction'
            'request_product_search'
            'explanation_product_trend'
            'request_product'
            'approval'
            'request_product_search_seasonal-set'
            null
)

target_ary = %w(
              'true'
              'false'
)

db_name = 'oksky_chat'

#SCHEDULER.every '1d',:first_in => 0 do |job|
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
    result_url = "gspreadsheet://tani@solairo.co.jp//analyze_message_tag/#{tag_ary[i]}_#{target_ary[j]}" #<- シートを追加して上書き
    #result_url = "gspreadsheet://tani@solairo.co.jp//analyze_message_tag/sheet1?mode=append" #<-追記モードの仕様を捜索中
    #"s3://accesskey:secretkey@/bucketname/path/to/file.csv.gz?compression=gz"
    client.query(db_name,sql,result_url)
  end   #2.times END
end   #tag_ary.size.times END
#end #SCHEDULER END