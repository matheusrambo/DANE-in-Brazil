load 'style.gnu'
set datafile separator ","
set output '/home/rambotcc/TCC/análise-dados/Figuras/figura8.eps'

set size 0.8, 0.6
set grid
set xdata time
set timefmt "%Y%m%d %H"
set xrange["20211105 16":"20220113 16"]
set key top  left 
set format x "%d/%m"
set yrange [0:100]

set xtics 3600 * 24 * 7 * 2
set xlabel ""
set ylabel "{/Helvetica-Bold % de dominios incapazes de}\n{/Helvetica-Bold suportar DANE corretamente}"

plot\
"/home/rambotcc/TCC/análise-dados/stats-codes/valid-dn-stat-saopaulo.txt" u 1:(100*($4/$2)) w st linestyle 1 lw 3 title "{/Courier-Bold .br}"
