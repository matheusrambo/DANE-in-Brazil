load 'style.gnu'

set datafile separator ","
set output '/home/rambotcc/TCC/análise-dados/Figuras/figura6.eps'

set lmargin 10
set size 0.6, 0.54  # 06 and 0.4


set grid
set xdata time
set timefmt "%Y%m%d %H"

set size 0.6, 0.5
set origin 0, 0
set ylabel "{/Helvetica-Bold % de servidores SMTP}\n{/Helvetica-Bold que falham para estabelecer}\n{/Helvetica-Bold uma conexao TLS}" offset 0, 0
set xlabel "{/Helvetica-Bold Date}"
set xrange["20211105 16":"20220113 16"]
set xtics 3600 * 24 * 7 * 2
set format x "%m/%d"
set key top left maxrows 3
set yrange [0:2]
set ytics 0.5


set label "{/Helvetica-Bold STARTTLS nao implementado}" at "20211105 16", 1.75
plot "/home/rambotcc/TCC/análise-dados/stats-codes/starttls-error-stat-saopaulo.txt" u 1:(100 * ($6)/($2 - $8 - $13 - $7)) w st linestyle 4 lw 3  title "",\

#set yrange[0:0.3]
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
#plot "/home/rambotcc/TCC/análise-dados/stats-codes/starttls-error-stat-saopaulo.txt" u 1:(100 * ($6+$9)/($2 - $8 - $13 - $7)) w stlinestyle 1 lw 3  title "Sao-Paulo"
#"data/starttls-error-case-incheon.txt" u 1:(100 * ($6+$9)/($2 - $8 - $13 - $7)) w st linestyle 6 lw 3  title "incheon",\
