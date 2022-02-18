load 'style.gnu'

set datafile separator ","
set output '/home/rambotcc/TCC/análise-dados/Figuras/figura4.eps'

set grid
set xdata time
set timefmt "%Y-%m-%d"

set ylabel "{/Helvetica-Bold % de dominios que}\n{/Helvetica-Bold implantaram totalmente}\n{/Helvetica-Bold registros }{/Courier-Bold TLSA}" #offset 0, 0.5
set xlabel "{/Helvetica-Bold Data}"
set xrange["2021-11-05":"2022-01-13"]
set xtics 3600 * 24 * 7 * 2
set format x "%d/%m"
set yrange [0:100]
set key bottom left 
set size 0.6, 0.4
set origin 0, 0

plot\
"tlsa-counts.csv" u 1:(100*($5/$3)) w st linestyle 1 lw 3 title "{/Courier-Bold .br}",\