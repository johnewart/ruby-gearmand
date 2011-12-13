class Worker
  include MongoMapper::Document
  
  key :worker_id, String
  key :ip_address, String
  key :port, Integer
  key :active, Boolean
  key :capabilities,  Array, :typecast => 'String'
  key :last_seen, Time
  
  
  #has_and_belongs_to_many :job_queues
  #has_many :worker_heartbeats

  def last_seen
    #WorkerHeartbeat.find(:first, :conditions => ["worker_id = :worker_id", {:worker_id => self.id}], :order => "created_at DESC").created_at rescue nil
  end

end
