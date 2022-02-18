load 'style.gnu'

set datafile separator ","
set output '/home/rambotcc/TCC/análise-dados/Figuras/figura5.eps'

set lmargin 8
set size 0.6, 0.6  # 06 and 0.4
set multiplot

set grid
set xdata time
set timefmt "%Y%m%d %H"

set size 0.6, 0.33
set origin 0, 0
set ylabel "{/Helvetica-Bold % de registros }{/Courier-Bold TLSA }{/Helvetica-Bold assinados}" offset 0, 5
set xlabel "{/Helvetica-Bold Date}"
set format x ""
set xrange["20211105 16":"20220113 16"]
set xtics 3600 * 24 * 7 * 2
set format x "%m/%d"
set key top left maxrows 3
set yrange [0:5]

set label "{/Helvetica-Bold Sem }{/Helvetica-Bold registros }{/Courier-Bold DS }" at "20211105 16", 3
plot "/home/rambotcc/TCC/análise-dados/stats-codes/dnssec-stat-saopaulo.txt" u 1:(100 * ( ($6) / ($4 + $6) )) w st linestyle 5 lw 3 title "Sao-Paulo",\

set yrange [60:100]
set size 0.6, 0.29
set origin 0, 0.32
set xtics()
unset label
set xlabel ""
set ylabel ""
set format x ""
set xrange["20211105 16":"20220113 16"]
set xtics 3600 * 24 * 7 * 2
set format x "%m/%d"
set label "{/Helvetica-Bold % de registros }{/Courier-Bold TLSA }{/Helvetica-Bold assinados" at "20211105 16", 73
plot "/home/rambotcc/TCC/análise-dados/stats-codes/dnssec-stat-saopaulo.txt" u 1:(100 * ( ($4 + $6) / ($2 - $3) )) w st linestyle 1 lw 3 title "",\


#set yrange [0:0.3]
#set ytics 0.1

#date	 total	 crawled	 notCrawled	4XX
#50X	55X	 tcp	 tls	1XX
#2XX	3XX	 etc
#
# 55X ($7): spam errors
# Successful SMTP connections w/ spam: let a:= ($2 - $8 - $13 - $7)
# temporal errors: 4XX:  The server has encountered a temporary failure.
    # thus SMTP errors:  $5 / a 
# 50X: when they do not understand command and TLS errors ($6 + $9)
#"data/starttls-error-case-virginia.txt" u 1:(100 * $5/($2 - $8 - $13 - $7)) w st linestyle 1 lw 3  title "temporary failure",\
#plot "data/starttls-error-case-virginia.txt" u 1:(100 * ($6+$9)/($2 - $8 - $13 - $7)) w stlinestyle 1 lw 3  title "Virginia"
#"data/starttls-error-case-incheon.txt" u 1:(100 * ($6+$9)/($2 - $8 - $13 - $7)) w st linestyle 6 lw 3  title "incheon",\
