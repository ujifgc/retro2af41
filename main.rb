#coding:utf-8

# ATTENTION! If you are using non-ascii characters in your database, 
# please invoke `chcp 65001 before using this script

$dname = "log-#{DateTime.now.strftime("%Y%m%d%H%M%S")}"
Dir.mkdir $dname

trap :INT do
  puts "\nBreak requested. Wait for it"
  @stop = true
end

ms = MSSQL.new
ms.connect

login_query = "
  create table #t_user_var(
    uisn integer, 
    lisn integer null,
    prot_flags varchar(255) default ',' )

  INSERT INTO #t_user_var (uisn) 
  SELECT isn_lclassif 
  FROM USER_CL
  WHERE upper(ORACLE_ID)='AF41'
"
ms.execute login_query

absent = {}
absent[:fund] = {}
absent[:inventory] = {}

present = {}
present[:unit] = {}
error = {}

new_unit_isn = 1 + ms.value( 'SELECT MAX(ISN_UNIT) FROM UNIT' ).to_i
new_weight = 1 + ms.value( 'SELECT MAX(WEIGHT) FROM UNIT' ).to_i

new_unit_isn = 400001  if new_unit_isn <= 400000
new_weight = 200001  if new_weight <= 200000

@logF = Logger.new $dname + '/search-fund.log'
@logI = Logger.new $dname + '/search-inventory.log'
@logU = Logger.new $dname + '/search-unit.log'
@logB = Logger.new $dname + '/binary.log'

txt_dir = ARGV[0]
unless txt_dir.to_s.length > 0
  ap "Не указан каталог с описями"
  exit
end
files = Dir.glob("#{txt_dir}/*.txt")
unless files.count > 0
  ap "В каталоге '#{txt_dir}' нет файлов описей '*.txt'"
  exit
end

gbar = ProgressBar.new('all', 1) 
files.each do |file|
  break  if @stop
  file.encode! 'UTF-8'
  begin
    query = ''
    line = ''

    fund_name, inventory_name = File.basename(file, '.*').split('-')
    lines = File.read(file).force_encoding('UTF-8').lines.to_a
    pbar = ProgressBar.new(file, lines.count - 3) 

    fund_result = ms.query_table "SELECT * FROM FUND WHERE FUND_NUM_2='#{fund_name}'"
    if fund_result.any?
      fund = fund_result.first
    else
      @logF.error "Внимание: фонд #{fund_name} не найден"
      absent[:fund].merge! fund_name => fund_name
    end

    inventory_name_1 = inventory_name.to_i
    inventory_name_2 = inventory_name.gsub(/\d+/,'')
    q = "SELECT * FROM INVENTORY WHERE ISN_FUND='#{fund['ISN_FUND']}' AND INVENTORY_NUM_1='#{inventory_name_1}' AND ISNULL(INVENTORY_NUM_2, '')='#{inventory_name_2}'"
    inventory_result = ms.query_table q
    if inventory_result.any?
      inventory = inventory_result.first
      inventory_isn = inventory['ISN_INVENTORY']
    else
      @logI.error "Внимание: опись #{inventory_name} фонда #{fund_name} не найдена"
      absent[:inventory].merge! inventory_name => [inventory_name_1, inventory_name_2]
    end

    unit_result = ms.query_table "SELECT * FROM UNIT WHERE ISN_INVENTORY='#{inventory['ISN_INVENTORY']}'"
    if unit_result.any?
      @logU.warn "Внимание: фонд #{fund_name.to_s.rjust(6)}, опись #{inventory_name.to_s.rjust(3)} уже содержит #{unit_result.count.to_s.rjust(4)} дел, файл #{file.to_s.ljust(12)} пропущен"
      present[:unit].merge! file => unit_result
      next
    else
      @logU.info "Внесение #{lines[4..-1].count} строк из файла #{file}"
    end

    lines[4..-1].each do |line|

      nn, name, dates, lists, info = line.strip.split(/\t/)
      dates = dates.split('-')  rescue []

      unit_name = name.strip.gsub "'", "''"
      unit_num_1 = nn.to_i
      unit_num_2 = nn.gsub(/\d+/,'')
      year_a = "'"+dates[0].rpartition('.').last+"'"  rescue 'NULL'
      year_b = "'"+dates[1].rpartition('.').last+"'"  rescue 'NULL'
      unit_page_count = lists.to_i
      unit_dates = dates.join(' - ')

      # here goes SQL MAGIC
      
      query = "
INSERT INTO [dbo].[UNIT]
 ([ISN_UNIT] ,[ISN_HIGH_UNIT] ,[ISN_INVENTORY] ,[ISN_DOC_TYPE] ,[ISN_LOCATION]
 ,[ISN_SECURLEVEL] ,[SECURITY_CHAR] ,[SECURITY_REASON] ,[ISN_INVENTORY_CLS] ,[ISN_STORAGE_MEDIUM]
 ,[ISN_DOC_KIND] ,[UNIT_KIND] ,[UNIT_NUM_1] ,[UNIT_NUM_2] ,[VOL_NUM]
 ,[NAME] ,[ANNOTATE] ,[DELO_INDEX] ,[PRODUCTION_NUM] ,[UNIT_CATEGORY]
 ,[NOTE] ,[IS_IN_SEARCH] ,[IS_LOST] ,[HAS_SF] ,[HAS_FP]
 ,[HAS_DEFECTS] ,[ARCHIVE_CODE] ,[CATALOGUED] ,[WEIGHT] ,[UNIT_CNT]
 ,[START_YEAR] ,[START_YEAR_INEXACT] ,[END_YEAR] ,[END_YEAR_INEXACT] ,[MEDIUM_TYPE]
 ,[BACKUP_COPY_CNT] ,[HAS_TREASURES] ,[IS_MUSEUM_ITEM] ,[PAGE_COUNT] ,[CARDBOARDED]
 ,[ADDITIONAL_CLS] ,[ALL_DATE] ,[ISN_SECURITY_REASON])
 VALUES (
 '#{new_unit_isn}',NULL,'#{inventory_isn}',1,NULL,
 1,'o',NULL,'#{inventory_isn}',NULL,
 NULL,703,'#{unit_num_1}','#{unit_num_2}',NULL,
 '#{unit_name}',NULL,NULL,NULL,'b',
 NULL,'N','N','N','N',
 'N',NULL,'N','#{new_weight}',NULL,
 #{year_a},NULL,#{year_b},NULL,'T',
 NULL,'N','N','#{unit_page_count}',NULL,
 '','#{unit_dates}',NULL)
"

      new_unit_isn += 1
      new_weight += 1

      ms.insert query

      pbar.inc

      # end SQL MAGIC

    end
    pbar.finish
    ms.part
  rescue TinyTds::Error => e
    ap e
    error[new_unit_isn] = [e,query,line]
    @logU.error [e,query,line].inspect
  end
end
gbar.finish

@logB << "\r\n-- absent --\r\n"  + absent.inspect
@logB << "\r\n-- present --\r\n" + present.inspect
@logB << "\r\n-- error --\r\n"   + error.inspect

ms.close
