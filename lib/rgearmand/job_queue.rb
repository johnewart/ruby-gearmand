module Rgearmand
    class JobQueue

      def initialize(queue, options = {:hostname => `hostname`.chomp, :record_stats => false})
        logger.debug "MongoDB Queue starting up"
        
        @hostname     = options[:hostname]
        @record_stats = options[:record_stats]
      end

      def retrieve_next(func_name, priority)
        job = Job.first(:func_name => func_name, :priority => priority, :timestamp.lt => Time.now().to_f, :completed_at => nil, :started_at => nil)

        if job != nil
          logger.debug "Found job: #{job.inspect}"
          job.update_attributes({:started_at => Time.now().to_f})
        end 

        job
      end

      def store!(func_name, data, uniq, timestamp, priority, job_handle)
        
        logger.debug("Storing for job queue: #{func_name}")
        
        job_attributes = {
          :uniq => uniq,
          :func_name => func_name,
          :data => data,
          :uniq => uniq,
          :timestamp => timestamp, 
          :priority => priority,
          :job_handle => job_handle
        }

        logger.debug "Storing job in Mongo DB: #{job_attributes}"

        begin
          job = Job.first(:uniq => uniq, :func_name => func_name, :started_at => nil)
          job.update_attributes(job_attributes) 
        rescue 
          job = Job.create(job_attributes)
        end
          
        return job
      end

      def delete!(job)
        job = Job.find(job.id)
        if job != nil
          if @record_stats
            job.complete! 
          else
            job.destroy
          end
        end
      end
   
  end
end
