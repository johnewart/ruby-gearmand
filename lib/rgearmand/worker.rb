class Worker
  include MongoMapper::Document
  
  key :worker_id, String
  key :ip_address, String
  key :port, Integer
  key :active, Boolean
  key :capabilities,  Array, :typecast => 'String'
  key :last_seen, Time

end
