#coding:utf-8

j = 0
Dir.open('.').each do |dir|
  next  unless dir.match(/^\d+\.done$/)
  p dir
  File.open "#{dir.to_i}.stat.log", 'w' do |out|
    out.write( "%5s\t%5s\t%8s\n" % ["Фонд", "Опись", "Записи"] )
    j = 0
    Dir.open(dir).sort_by{|n|n.split('.')[0].split('-')[0].to_i rescue 0}.each do |name|
      name.encode! 'utf-8'
      if name.match /\.txt$/
        buf = File.read( dir + '/' + name )
        k = 0
        buf.each_line do |line|
          if line.match /\d+\t.*$/
            k += 1
          else
            p line
          end
        end
        fund, inv = name.rpartition('.')[0].split('-')
        if inv[-1].match /\d/
          out.write( "%5s\t%5s\t%8d\n" % [fund.to_s, inv.to_s, k] )
        else
          out.write( "%5s\t%6s\t%8d\n" % [fund.to_s, inv.to_s, k] )
        end
        j += k
      end
    end
    out.write "Итого\tзаписей\t%8d\n" % [j]
  end
end
