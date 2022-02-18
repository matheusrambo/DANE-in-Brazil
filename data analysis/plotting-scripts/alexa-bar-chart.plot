load 'plots/style.gnu'
set datafile separator ","

set size 0.6, 0.4
set grid
set key top right
set yrange [0:]
set xrange [0:85]

set xlabel "{/Helvetica-Bold Alexa Site Rank (bins of 10,000)}"
set ylabel "{/Helvetica-Bold % of domains with}\n{/Courier-Bold MX }{/Helvetica-Bold and }{/Courier-Bold TLSA }{/Helvetica-Bold records}"
set xtics ("0" 0, "200k" 20, "400k" 40, "600k" 60, "800k" 80, "1M" 100)

plot \
"data/alexa-top1m-tlsa-deployed.txt" u ($1+1):(100*($3/$2)) w boxes linecolor rgb "dark-red" title ""

