# Initialize

require 'td-client'


module DBClient
  class TD
    def initialize(apikey: nil)
      apikey = '***************' unless apikey
      @client = TreasureData::Client.new apikey
      @job_num
    end
    attr_reader :headers,:job_num
    def wait_finished db_name,sql,result_url,type, &block
      job = @client.query(db_name, sql,result_url,
                          priority=nil, retry_limit=nil,
                          opts={type: type})
      puts "waiting until job_id:#{job.job_id} finished... "
      @job_num = job.job_id
      until job.finished?
        sleep 2
        job.update_progress!
      end
      job.update_status!
      puts "job_id:#{job.job_id} finished, status:#{job.status} "
      block.call(job)
    end
#=begin
    def query(db_name, sql,result_url=nil,type: nil)
      type ||= :hive
      results = []
      wait_finished(db_name,sql,result_url,type) do |job|
        job.result_each do |row|
          results << {label: row[0], value: row[1]}
        end
        @headers = results.map{|m| m[:label]} unless @headers
      end
      results
    end #def query END
#=end
    def job_export(job_id)
      job = job_id.to_i
     p   @client.result_export(job)

    end
  end
end

