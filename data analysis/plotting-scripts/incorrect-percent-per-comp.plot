load 'style.gnu'

set datafile separator ","
set output '/home/rambotcc/TCC/análise-dados/Figuras/figura7.eps'

set grid
set xdata time
set timefmt "%Y%m%d %H"

set ylabel "{/Helvetica-Bold % de }{/Helvetica-Bold registros}{/Courier-Bold TLSA }\n{/Helvetica-Bold que falham na validacao}" #offset 0, 5
set xlabel "{/Helvetica-Bold Data}"
set xrange["20211105 16":"20220113 16"]
set xtics 3600 * 24 * 14
set format x "%d/%m"
#set ytics 0.2
set yrange[0:20]
set key top right
#set label "{/Helvetica-Bold DNSSEC}" at "20211105 16", 0.1

#time, totalDn, noData, bogus, wrongChain, usage2, usage3, undefined, selector, matchingType, notmatch-tlsa-certificate
#TLSA Errors: Wrong usage,  selector, undefined
plot \
"/home/rambotcc/TCC/análise-dados/stats-codes/check-incorrect-stat-saopaulo.txt" u 1:(100*$5/($2-$3-$4)) w  st linestyle 1 lw 3  title "DNSSEC",\
"/home/rambotcc/TCC/análise-dados/stats-codes/check-incorrect-stat-saopaulo.txt" u 1:(100*($7+$8+$9+$10+$11)/($2-$3-$4)) w  st linestyle 2 lw 3  title "Certificate"
