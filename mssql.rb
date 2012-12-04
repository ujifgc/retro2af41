#coding:utf-8
class MSSQL
  attr_accessor :connection
  
  def initialize( opts = {} )
    yml = YAML.load_file('mssql.yml')['development']
    @parameters = {}
    yml.each{ |k,v| @parameters[k.to_sym] = v.to_s }
    @connection = nil
    @log = Logger.new $dname + '/mssql-query.log'
    @logi = Logger.new $dname + '/mssql-insert.log'
  end

  def connect
    @connection = TinyTds::Client.new @parameters
    if @connection.closed?
      raise Exception, 'connection error'
    else
      ap "Соединение успешно"
    end
  end

  def close
    @connection.close
  end

  def execute( q )
    @log.info 'EXECUTE ' + q.gsub(/[\n\r\s]+/, ' ')
    @connection.execute q
  end

  def delete( q )
    @log.info 'DELETE ' + q.gsub(/[\n\r\s]+/, ' ')
    result = @connection.execute q
    result.do
  end

  def insert( q )
    @logi.info 'INSERT ' + q.gsub(/[\n\r\s]+/, ' ')
    result = @connection.execute q
    result.insert
  end

  def part
    @logi.info ' '
    @logi.info ' '
  end

  def query_table( q )
    @log.info 'TABLE ' + q.gsub(/[\n\r\s]+/, ' ')
    connect  if !@connection || @connection.closed?
    result = @connection.execute q
    table = result.to_a
    result.cancel
    table
  end

  def query_value( q )
    @log.info 'VALUE ' + q.gsub(/[\n\r\s]+/, ' ')
    connect  if !@connection || @connection.closed?
    result = @connection.execute q
    value = result.first.first[1]
    result.cancel
    value.kind_of?( Array ) ? value[1] : value
  end
  alias value query_value

  def query( q )
    @log.info 'BLOCK ' + q.gsub(/[\n\r\s]+/, ' ')
    if block_given?
      connect  if !@connection || @connection.closed?
      result = @connection.execute q
      result.each do |row|
        yield row
      end      
      result.cancel
    else
      raise Exception, 'no blocks given'
    end
  end

end
