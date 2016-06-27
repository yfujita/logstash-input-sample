# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Sample < LogStash::Inputs::Base
  config_name "sample"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # The message string to use in the event.
  config :option, :validate => :string, :default => ""

  # Set how frequently messages should be sent.
  #
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 30

  config :tmpfile, :validate => :string, :default => "/tmp/logstash-dstat.csv"

  public
  def register
    @logger.info("Registering Sampl Input", :type => @type, :command => @option, :interval => @interval)
    @host = Socket.gethostname
    @command = 'dstat ' + option + ' --output ' + @tmpfile + ' 1 0'
  end # def register

  def run(queue)
    # we can abort the loop if stop? becomes true
    while !stop?
      touch_or_truncate @tmpfile
      lines = exec_dstat(@command, @tmpfile)
      events = create_dstat_event lines
      events.each{|event|
        decorate(event)
        queue << event
      }
      # because the sleep interval can be big, when shutdown happens
      # we want to be able to abort the sleep
      # Stud.stoppable_sleep will frequently evaluate the given block
      # and abort the sleep(@interval) if the return value is true
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end # def run

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end

  def exec_dstat(cmd, tmpfile)
    @logger.debug? && @logger.debug("Running dstat", :command => cmd)
    begin
      `#{cmd}`
      File.open(tmpfile) do |file|
        file.read.split("\n")
      end
    rescue Exception => e
      @logger.error("Exception while running command",
        :command => option, :e => e, :backtrace => e.backtrace)
    ensure
      stop
    end
  end

  def touch_or_truncate(file)
    if File.exist?(file)
      File.truncate(file, 0)
    else
      `touch #{file}`
    end
  end

  def create_dstat_event(lines)
    events = []
    lines.each_with_index do |line, line_number|
      line.delete!("\"")
      next if line == ""
      case line_number
      when 0..4
      when 5
        @first_keys = CSV.parse_line(line)
        pre_key = ""
        @first_keys.each_with_index do |key, index|
          if key.nil? || key == ""
            @first_keys[index] = pre_key
          else
            @first_keys[index] = @first_keys[index].gsub(/\s/, '_')
          end
          pre_key = @first_keys[index]
        end
      when 6
        @second_keys = line.split(",")
      when 7
        CSV.parse_line(line).each_with_index do |value, index|
          key = resolve_key(@first_keys[index], @second_keys[index])
          if key != nil
            event = LogStash::Event.new("stat" => key, "value" => value, "host" => @host)
            events.push(event)
          end
        end
      end
    end

    events
  end

  def resolve_key(first_key, second_key)
    key_map = {}
    key_map['load_avg'] = {'1m' => 'loadavg-short', '5m' => 'loadavg-middle', '15m' => 'loadavg-long'}
    key_map['total_cpu_usage'] = {'usr' => 'cpu-usr', 'sys' => 'cpu-sys', 'idl' => 'cpu-idl', 'wai' => 'cpu-wai', 'hiq' => 'cpu-hiq', 'siq' => 'cpu-siq'}
    key_map['net/total'] = {'recv' => 'net-recv', 'send' => 'net-send'}
    key_map['/'] = {'used' => 'disk-used', 'free' => 'disk-free'}
    key_map['memory_usage'] = {'used' => 'mem-used', 'buff' => 'mem-buff', 'cach' => 'mem-cach', 'free' => 'mem-free'}
    key_map['dsk/total'] = {'read' => 'dsk-read', 'writ' => 'dsk-writ'}
    key_map['paging'] = {'in' => 'paging-in', 'out' => 'paging-out'}
    key_map['system'] = {'int' => 'sys-int', 'csw' => 'sys-csw'}
    key_map['swap'] = {'used' => 'swap-used', 'free' => 'swap-free'}
    key_map['procs'] = {'run' => 'procs-run', 'blk' => 'procs-blk', 'new' => 'procs-new'}

    key_map[first_key] ? key_map[first_key][second_key] : nil
  end

end # class LogStash::Inputs::Example
