#require File.expand_path('manager', File.dirname(__FILE__))
require 'rgearmand/job'
require 'rgearmand/worker'

require 'rgearmand/client_requests'
require 'rgearmand/worker_requests'
require 'rgearmand/em_adapter'
require 'rgearmand/worker_queue'
require 'rgearmand/job_queue'


module Rgearmand
  class GearmanServer < EventMachine::Connection
    include Rgearmand::EmAdapter
    include Rgearmand::ClientRequests
    include Rgearmand::WorkerRequests
  
    def initialize
      @capabilities = Set.new
    end
    
    def self.worker_queue
      @worker_queue
    end
    
    def worker_queue
      self.class.worker_queue
    end
  
    def self.start
      @hostname = HOSTNAME
      logger.debug "Hostname: #{@hostname}"

      @port = nil
      @worker_queue = WorkerQueue.new(@hostname)
      @persistent_queue = JobQueue.new(@worker_queue, {:record_stats => false})
      @worker_queue.persistent_queue = @persistent_queue 
      
      EventMachine::run {
    
        logger.info "Starting server on #{OPTIONS[:ip]}:4730"
        EventMachine::start_server OPTIONS[:ip], 4730, self
    
        Rgearmand.call_after_inits
      }
    end
  end
end
