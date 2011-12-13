module Rgearmand
  module LoadBalance
    def wakeup_workers(func)
      worker_queue.each_worker(func_name) do |w|
        logger.debug "Sending NOOP to #{w}"
        packet = generate :noop
        w.send_data(packet)
      end
    end
    
    
  end
end