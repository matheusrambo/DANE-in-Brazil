load 'style.gnu'

set datafile separator ","
set output '/home/rambotcc/TCC/análise-dados/Figuras/figura 5.eps'

set grid
set xdata time
set timefmt "%Y%m%d %H"

set ylabel "{/Helvetica-Bold % de }{/Helvetica-Bold registros }{/Courier-Bold TLSA }\n{/Helvetica-Bold assinados}" #offset 0, 5
set xlabel "{/Helvetica-Bold Data}"
set xrange["20211105 16":"20220113 16"]
set xtics 3600 * 24 * 7 * 2
set format x "%d/%m"
set yrange [80:90]
set key top right
#set size 0.6, 0.29
set origin 0, 0.32

#set label "{/Helvetica-Bold % de registros }{/Courier-Bold TLSA }{/Helvetica-Bold assinados" at "20211105 16", 73
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
