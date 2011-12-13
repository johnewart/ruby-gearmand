module Rgearmand
  module EmAdapter
    def initialize
      logger.info "Connection from someone..."
      @capabilities = []
      @currentjob = nil
      @type = nil
      @current_packet = nil
      super
    end

    # Overrides for connections

    def post_init
      @source_port, @source_ip = Socket.unpack_sockaddr_in(get_peername)
      @local_port, @local_ip = Socket.unpack_sockaddr_in(get_sockname)
      logger.debug "-- someone connected to rgearmand  from #{@source_ip}:#{@source_port}"
    end
        
    def unbind
      if @type == :worker
        logger.debug "-- a worker disconnected!"
        
        w = Worker.first(:worker_id => @worker_id)
        if w != nil
          logger.debug "Found worker: #{w.inspect}"
          w.active = false
          w.save
        end
      else
        logger.debug "-- a client disconnected!"
      end
    end

    def receive_data(data)
      logger.debug "Received data: #{data.inspect} with length #{data.length}"

      if @current_packet == nil && data.bytes.to_a[0] != 0
        logger.debug "control message <<< #{data.inspect}"
        Rgearmand.control_packet(data)
      else
        offset = 0

        if @current_packet != nil
          pkt_to_read = @current_packet[:length] - @current_packet[:data].length
          logger.debug("Continuation of a previous packet! #{@current_packet[:length]} bytes expected in packet, #{data.length} read this time #{pkt_to_read} to read")
          current_len = @current_packet[:data].length
          @current_packet[:data] += data[0..pkt_to_read-1]

          # Offset is now the end of this continued packet
          offset = pkt_to_read 
          
          if @current_packet[:data].length == @current_packet[:length]
            args = @current_packet[:data].split("\0")
            cmd = @current_packet[:cmd]

            self.send(cmd, *args)
            @current_packet = nil
          end
        end

        if @current_packet == nil 
          while(offset < data.length)
            @current_packet = {}
            header = data[offset+0..offset+11]

            # Parse packet header
            @current_packet[:type]    = header[1..3].to_s
            @current_packet[:cmd]     = COMMANDS[header[4..7].unpack('N').first]
            @current_packet[:length]  = header[8..11].unpack('N').first

            # Advance past header
            offset += 12

            if @current_packet[:length] >= data.length - 12
              # If the data portion is longer than the number of 
              # bytes we've received then use the data length 
              # as the end (- 12 for header)
              end_byte_offset = data.length 
            else 
              # Otherwise, use the length of the packet (2 packets in one)
              logger.debug "Multiple packets in one read"
              end_byte_offset = @current_packet[:length] + offset
            end

            if end_byte_offset <= offset
              @current_packet[:data]  = ""
            else 
              @current_packet[:data]  = data[offset..end_byte_offset-1]
            end

            # Advance to end of this packet
            offset = end_byte_offset 

            if @current_packet[:data].length == @current_packet[:length]
              args = @current_packet[:data].split("\0")
              cmd = @current_packet[:cmd]

              self.send(cmd, *args)
              @current_packet = nil
            end
          end        
        end
      end
    end
    
    def generate(name, *args)
      args = args.flatten
      num = COMMAND_INV[name]
      arg = args.join("\0")
      data = [
        "\0",
        "RES",
        [num, arg.size].pack('NN'),
        arg
      ].join
    end

    def respond(type, *args)
      packet = generate(type, *args)
      logger.debug "response >>> #{packet.inspect}"
      send_data(packet)
    end
    
    def send_client(packet_type, job_handle, *args)
      packet = generate(packet_type, job_handle, args)
      worker_queue.client(job_handle){|c| c.send_data packet}
    end
    
  end
end
