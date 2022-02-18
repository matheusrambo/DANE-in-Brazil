load 'style.gnu'

set datafile separator ","
set output '/home/rambotcc/TCC/análise-dados/Figuras/figura2.eps'

set grid
set xdata time
set timefmt "%Y-%m-%d"


set ylabel "{/Helvetica-Bold % de dominios com}\n{/Helvetica-Bold registros }{/Courier-Bold TLSA}" #offset 0, 0.5
set xlabel "{/Helvetica-Bold Data}"
set xrange["2021-11-05":"2022-01-13"]
set xtics 3600 * 24 * 7 * 2
set format x "%d/%m"
set yrange [0:0.3]
set key top left 
set size 0.6, 0.4
set origin 0, 0

plot\
"tlsa-counts.csv" u 1:(100*($3/$2)) w st linestyle 1 lw 3 title "{/Courier-Bold .br}"



