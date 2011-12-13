class Job 
  include MongoMapper::Document
    
  key :job_queue, String
  key :timestamp, Float
  key :result, String
  key :status, String
  key :completed_at, Float
  key :started_at, Float
  key :runtime, Float
  key :data, String
  
  def complete!
    self.completed_at = Time.now().to_f
    self.runtime = self.completed_at - self.started_at
    self.save!
  end

  def restart
    self.timestamp = 0
    self.runtime = nil
    self.started_at = nil
    self.completed_at = nil
    self.result = nil
    self.status = nil
    self.save
  end

end
