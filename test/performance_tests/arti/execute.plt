#!/usr/bin/gnuplot --persist

datafile=ARG1
print "filename = ", datafile

set terminal postscript eps enhanced color
set output datafile.'.eps'

set style data line
set autoscale fix
set key off

stats datafile using 1:2 prefix "DS"
stats datafile using 1:3 prefix "DSRet"
stats datafile using 1:4 prefix "DSRetBuff"
stats datafile using 1:5 prefix "IB"
stats datafile using 1:6 prefix "IBRet"
stats datafile using 1:7 prefix "IBRetBuff"
stats datafile using 1:8 prefix "DSOnAll"
stats datafile using 1:9 prefix "IBOnAll"

set multiplot layout 4,2 title "Performance comparison AR-API"
set bmargin 2

set xlabel ""
set ylabel "Time (ms)"

set title "Data Structure"
plot datafile using 1:2 title "Data" lw 1, \
     DS_mean_y title "Mean" lw 2, \
     DS_slope * x + DS_intercept title "Linear Fit" lw 2

set title "Input Buffer"
plot datafile using 1:5 title "Data" lw 1, \
     IB_mean_y title "Mean" lw 2, \
     IB_slope * x + IB_intercept title "Linear Fit" lw 2

set title "Data Structure w/ Ret value"
plot datafile using 1:3 title "Data" lw 1, \
     DSRet_mean_y title "Mean" lw 2, \
     DSRet_slope * x + DSRet_intercept title "Linear Fit" lw 2

set title "InputBuffer w/ Ret"
plot datafile using 1:6 title "Data" lw 1, \
     IBRet_mean_y title "Mean" lw 2, \
     IBRet_slope * x + IBRet_intercept title "Linear Fit" lw 2

set title "Data Structure w/ Ret Buffer"
plot datafile using 1:4 title "Data" lw 1, \
     DSRetBuff_mean_y title "Mean" lw 2, \
     DSRetBuff_slope * x + DSRetBuff_intercept title "Linear Fit" lw 2

set title "InputBuffer w/ Ret"
plot datafile using 1:7 title "Data" lw 1, \
     IBRetBuff_mean_y title "Mean" lw 2, \
     IBRetBuff_slope * x + IBRetBuff_intercept title "Linear Fit" lw 2

set title "Data Structure On All"
plot datafile using 1:8 title "Data" lw 1, \
     DSOnAll_mean_y title "Mean" lw 2, \
     DSOnAll_slope * x + DSOnAll_intercept title "Linear Fit" lw 2

set title "InputBuffer On All"
plot datafile using 1:9 title "Data" lw 1, \
     IBOnAll_mean_y title "Mean" lw 2, \
     IBOnAll_slope * x + IBOnAll_intercept title "Linear Fit" lw 2

unset multiplot

set output datafile.'.bar.eps'
set style fill solid 1.00 border 0

set xrange [0:9]
set yrange [0:2000]
barSize = 0.5
set title "Performance Comparison between structs and input buffers"

plot '' u (1):(DS_mean_y):(barSize) w boxes ls 2,                     \
     '' u (1):(DS_mean_y):(DS_stddev_y) w yerrorb ls 1,               \
     '' u (2):(IB_mean_y):(barSize) w boxes ls 3,                     \
     '' u (2):(IB_mean_y):(IB_stddev_y) w yerrorb ls 1,               \
     '' u (3):(DSRet_mean_y):(barSize) w boxes ls 2,                  \
     '' u (3):(DSRet_mean_y):(DSRet_stddev_y) w yerrorb ls 1,         \
     '' u (4):(IBRet_mean_y):(barSize) w boxes ls 3,                  \
     '' u (4):(IBRet_mean_y):(IBRet_stddev_y) w yerrorb ls 1,         \
     '' u (5):(DSRetBuff_mean_y):(barSize) w boxes ls 2,              \
     '' u (5):(DSRetBuff_mean_y):(DSRetBuff_stddev_y) w yerrorb ls 1, \
     '' u (6):(IBRetBuff_mean_y):(barSize) w boxes ls 3,              \
     '' u (6):(IBRetBuff_mean_y):(IBRetBuff_stddev_y) w yerrorb ls 1, \
     '' u (7):(DSOnAll_mean_y):(barSize) w boxes ls 2,                \
     '' u (7):(DSOnAll_mean_y):(DSOnAll_stddev_y) w yerrorb ls 1,     \
     '' u (8):(IBOnAll_mean_y):(barSize) w boxes ls 3,                \
     '' u (8):(IBOnAll_mean_y):(IBOnAll_stddev_y) w yerrorb ls 1
