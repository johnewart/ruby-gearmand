require "rubygems"
require "bundler"

Bundler.setup
Bundler.require(:default, GEARMAN_ENV)

MongoMapper.connection = Mongo::Connection.new('localhost', 27017)
MongoMapper.database = "rgearmand-#{GEARMAN_ENV}"

$: << File.dirname(__FILE__)
Debugger.start if GEARMAN_ENV == 'development'

COMMANDS = {
  1  => :can_do,               # W->J: FUNC
  2  => :cant_do,              # W->J: FUNC
  3  => :reset_abilities,      # W->J: --
  4  => :pre_sleep,            # W->J: --
  #5 =>  (unused),             # -      -
  6  => :noop,                 # J->W: --
  7  => :submit_job,           # C->J: FUNC[0]UNIQ[0]ARGS
  8  => :job_created,          # J->C: HANDLE
  9  => :grab_job,             # W->J: --
  10 => :no_job,               # J->W: --
  11 => :job_assign,           # J->W: HANDLE[0]FUNC[0]ARG
  12 => :work_status,          # W->J/C: HANDLE[0]NUMERATOR[0]DENOMINATOR
  13 => :work_complete,        # W->J/C: HANDLE[0]RES
  14 => :work_fail,            # W->J/C: HANDLE
  15 => :get_status,           # C->J: HANDLE
  16 => :echo_req,             # ?->J: TEXT
  17 => :echo_res,             # J->?: TEXT
  18 => :submit_job_bg,        # C->J: FUNC[0]UNIQ[0]ARGS
  19 => :error,                # J->?: ERRCODE[0]ERR_TEXT
  20 => :status_res,           # C->J: HANDLE[0]KNOWN[0]RUNNING[0]NUM[0]DENOM
  21 => :submit_job_high,      # C->J: FUNC[0]UNIQ[0]ARGS
  22 => :set_client_id,        # W->J: [RANDOM_STRING_NO_WHITESPACE]
  23 => :can_do_timeout,       # W->J: FUNC[0]TIMEOUT
  24 => :all_yours,            # REQ    Worker
  25 => :work_exception,       # W->J: HANDLE[0]ARG
  26 => :option_req,           # C->J: TEXT
  27 => :option_res,           # J->C: TEXT
  28 => :work_data,            # REQ    Worker
  29 => :work_warning,         # W->J/C: HANDLE[0]MSG
  30 => :grab_job_uniq,        # REQ    Worker
  31 => :job_assign_uniq,      # RES    Worker
  32 => :submit_job_high_bg,   # C->J: FUNC[0]UNIQ[0]ARGS
  33 => :submit_job_low,       # C->J: FUNC[0]UNIQ[0]ARGS
  34 => :submit_job_low_bg,    # C->J: FUNC[0]UNIQ[0]ARGS
  35 => :submit_job_sched,     # REQ    Client
  36 => :submit_job_epoch      # C->J: FUNC[0]UNIQ[0]EPOCH[0]ARGS
}
COMMAND_INV = COMMANDS.invert

# Signals
Signal.trap('INT') { EM.stop }
Signal.trap('TERM'){ EM.stop }

# TODO: Need a round-robin queue for workers.
module Rgearmand
  def options(&block)
    (@optionses ||= []) << block
  end
  module_function :options
  
  def call_optionses(object)
    if @optionses != nil
      @optionses.each{|c| c.call(object)}
    end
  end
  module_function :call_optionses
  
  def after_init(&block)
    (@after_inits ||= []) << block
  end
  module_function :after_init
  
  def call_after_inits
    if @after_inits != nil
      @after_inits.each{|c| c.call}
    end
  end
  module_function :call_after_inits
  
  def packet_match(string, &block)
    (@packet_matches ||= {}).merge!(string => block)
  end
  module_function :packet_match
  
  def control_packet(data)
    logger.debug "Looking for handler for control packet: #{data}"
    logger.debug @packet_matches.inspect
    begin 
      @packet_matches.each do |s, proc|
        logger.debug "#{s.size}, '#{data[0..(s.size-1)]}', '#{s}'"
        if data[0..(s.size - 1)] == s
          logger.debug "Found it!"
          proc.call(data)
        end
      end
    rescue
      nil
    end
  end  
  module_function :control_packet
end

require 'rgearmand/gearman_server'
