| Serializer                       | Records      | Run #     | Size [MB]  | Write [s]  | Read0 [s]  | Read 1 [s] | Read 2 [s] |
| ----------                       | ------:      | ----:     | --------:  | --------:  | --------:  | ---------: | ---------: |
| com.databricks.spark.csv         |       200000 |         0 |      26.12 |      3.189 |      1.397 |      1.510 |      1.444 |
| com.databricks.spark.avro        |       200000 |         0 |       4.37 |      4.338 |      1.344 |      1.074 |      1.082 |
| parquet                          |       200000 |         0 |       1.18 |      3.402 |      0.387 |      0.438 |      0.345 |
| com.databricks.spark.csv         |       400000 |         0 |      52.25 |      5.630 |      2.598 |      2.608 |      2.494 |
| com.databricks.spark.avro        |       400000 |         0 |       8.74 |      8.446 |      1.907 |      2.091 |      2.076 |
| parquet                          |       400000 |         0 |       2.36 |      5.872 |      0.341 |      0.521 |      0.556 |
| com.databricks.spark.csv         |       600000 |         0 |      78.37 |      8.428 |      3.805 |      3.703 |      3.593 |
| com.databricks.spark.avro        |       600000 |         0 |      13.10 |     11.939 |      2.764 |      2.741 |      2.917 |
| parquet                          |       600000 |         0 |       3.54 |      8.911 |      0.432 |      0.524 |      0.568 |
| com.databricks.spark.csv         |       800000 |         0 |     104.50 |     11.226 |      5.016 |      4.887 |      4.815 |
| com.databricks.spark.avro        |       800000 |         0 |      17.47 |     17.859 |      3.998 |      4.362 |      4.444 |
| parquet                          |       800000 |         0 |       4.72 |     16.151 |      0.675 |      0.794 |      0.884 |
| com.databricks.spark.csv         |      1000000 |         0 |     130.62 |     18.217 |      8.493 |      8.034 |      7.260 |
| com.databricks.spark.avro        |      1000000 |         0 |      21.84 |     23.210 |      6.141 |      6.482 |      7.318 |
| parquet                          |      1000000 |         0 |       5.90 |     20.652 |      1.409 |      1.706 |      1.695 |
| com.databricks.spark.csv         |      1200000 |         0 |     156.74 |     16.900 |     14.276 |     12.914 |     11.881 |
| com.databricks.spark.avro        |      1200000 |         0 |      26.21 |     23.733 |      8.801 |      8.636 |      8.013 |
| parquet                          |      1200000 |         0 |       7.07 |     17.042 |      0.941 |      1.403 |      1.381 |
| com.databricks.spark.csv         |      1400000 |         0 |     182.87 |     18.886 |     15.875 |     13.009 |     10.534 |
| com.databricks.spark.avro        |      1400000 |         0 |      30.58 |     28.345 |     10.305 |      9.984 |     10.171 |
| parquet                          |      1400000 |         0 |       8.25 |     20.351 |      0.956 |      1.518 |      1.334 |
| com.databricks.spark.csv         |      1600000 |         0 |     208.99 |     22.962 |     12.723 |     11.825 |     10.546 |
| com.databricks.spark.avro        |      1600000 |         0 |      34.95 |     34.767 |      8.751 |      9.574 |      8.560 |
| parquet                          |      1600000 |         0 |       9.43 |     24.033 |      0.818 |      1.184 |      1.203 |
| com.databricks.spark.csv         |      1800000 |         0 |     235.11 |     24.561 |     10.828 |     10.401 |     10.534 |
| com.databricks.spark.avro        |      1800000 |         0 |      39.32 |     45.808 |     11.247 |     13.687 |     13.374 |
| parquet                          |      1800000 |         0 |      10.60 |     35.511 |      0.952 |      1.468 |      1.266 |
| com.databricks.spark.csv         |      2000000 |         0 |     261.24 |     32.723 |     14.738 |     12.101 |     12.012 |
| com.databricks.spark.avro        |      2000000 |         0 |      43.69 |     38.586 |      8.828 |      8.849 |      8.954 |
| parquet                          |      2000000 |         0 |      11.78 |     29.530 |      1.158 |      1.791 |      1.561 |
| com.databricks.spark.csv         |      2200000 |         0 |     287.36 |     33.967 |     21.626 |     14.489 |     18.480 |
| com.databricks.spark.avro        |      2200000 |         0 |      48.06 |     43.756 |      9.151 |      9.902 |      9.654 |
| parquet                          |      2200000 |         0 |      12.96 |     31.467 |      1.201 |      1.863 |      1.578 |
| com.databricks.spark.csv         |      2400000 |         0 |     313.49 |     33.388 |     24.642 |     17.758 |     15.980 |
| com.databricks.spark.avro        |      2400000 |         0 |      52.42 |     57.874 |     13.287 |     12.236 |     14.214 |
| parquet                          |      2400000 |         0 |      14.14 |     39.847 |      1.422 |      1.993 |      1.975 |
| com.databricks.spark.csv         |      2600000 |         0 |     339.61 |     36.415 |     26.686 |     17.658 |     16.183 |
| com.databricks.spark.avro        |      2600000 |         0 |      56.79 |     51.941 |     11.113 |     11.940 |     11.560 |
| parquet                          |      2600000 |         0 |      15.32 |     37.729 |      1.367 |      3.017 |      2.160 |
| com.databricks.spark.csv         |      2800000 |         0 |     365.73 |     38.580 |     17.032 |     16.617 |     16.523 |
| com.databricks.spark.avro        |      2800000 |         0 |      61.16 |     55.527 |     12.997 |     12.835 |     13.013 |
| parquet                          |      2800000 |         0 |      16.50 |     41.002 |      1.184 |      2.185 |      1.818 |
| com.databricks.spark.csv         |      3000000 |         0 |     391.86 |     41.255 |     18.045 |     17.619 |     17.500 |
| com.databricks.spark.avro        |      3000000 |         0 |      65.53 |     60.523 |     13.911 |     13.511 |     13.772 |
| parquet                          |      3000000 |         0 |      17.68 |     44.196 |      1.254 |      1.961 |      1.690 |
| com.databricks.spark.csv         |      3200000 |         0 |     417.98 |     44.328 |     19.305 |     18.726 |     20.158 |
| com.databricks.spark.avro        |      3200000 |         0 |      69.90 |     63.829 |     13.789 |     14.335 |     15.025 |
| parquet                          |      3200000 |         0 |      18.85 |     49.605 |      1.296 |      1.871 |      2.049 |
| com.databricks.spark.csv         |      3400000 |         0 |     444.10 |     48.971 |     21.293 |     20.695 |     20.893 |
| com.databricks.spark.avro        |      3400000 |         0 |      74.26 |     73.190 |     16.459 |     15.878 |     15.454 |
| parquet                          |      3400000 |         0 |      20.03 |     52.778 |      1.343 |      2.014 |      2.028 |
| com.databricks.spark.csv         |      3600000 |         0 |     470.23 |     52.837 |     22.578 |     21.967 |     21.953 |
| com.databricks.spark.avro        |      3600000 |         0 |      78.63 |     78.038 |     19.146 |     18.609 |     19.127 |
| parquet                          |      3600000 |         0 |      21.21 |     55.509 |      1.493 |      2.170 |      2.211 |
| com.databricks.spark.csv         |      3800000 |         0 |     496.35 |     55.877 |     23.681 |     24.225 |     23.193 |
| com.databricks.spark.avro        |      3800000 |         0 |      83.00 |     78.107 |     17.294 |     17.109 |     17.502 |
| parquet                          |      3800000 |         0 |      22.39 |     55.668 |      1.420 |      2.347 |      1.979 |
| com.databricks.spark.csv         |      4000000 |         0 |     522.48 |     56.579 |     24.794 |     24.284 |     24.327 |
| com.databricks.spark.avro        |      4000000 |         0 |      87.37 |     82.245 |     19.216 |     19.894 |     18.409 |
| parquet                          |      4000000 |         0 |      23.57 |     57.920 |      1.544 |      2.332 |      2.292 |
| com.databricks.spark.csv         |      4200000 |         0 |     548.60 |     59.766 |     26.525 |     25.977 |     25.666 |
| com.databricks.spark.avro        |      4200000 |         0 |      91.74 |     89.976 |     21.572 |     19.675 |     19.911 |
| parquet                          |      4200000 |         0 |      24.74 |     62.840 |      1.585 |      2.584 |      2.569 |
| com.databricks.spark.csv         |      4400000 |         0 |     574.72 |     62.607 |     27.510 |     26.876 |     26.692 |
| com.databricks.spark.avro        |      4400000 |         0 |      96.11 |     91.673 |     19.419 |     19.886 |     20.540 |
| parquet                          |      4400000 |         0 |      25.92 |     66.483 |      1.724 |      2.681 |      2.639 |
| com.databricks.spark.csv         |      4600000 |         0 |     600.85 |     64.958 |     28.624 |     28.969 |     27.951 |
| com.databricks.spark.avro        |      4600000 |         0 |     100.47 |     94.673 |     21.337 |     21.819 |     21.309 |
| parquet                          |      4600000 |         0 |      27.10 |     68.862 |      1.909 |      3.333 |      3.548 |
| com.databricks.spark.csv         |      4800000 |         0 |     626.97 |     67.735 |     30.154 |     29.272 |     29.237 |
| com.databricks.spark.avro        |      4800000 |         0 |     104.84 |     99.045 |     21.063 |     21.904 |     22.028 |
| parquet                          |      4800000 |         0 |      28.27 |     71.069 |      1.769 |      2.761 |      2.770 |
| com.databricks.spark.csv         |      5000000 |         0 |     653.09 |     72.592 |     30.877 |     30.212 |     30.770 |
| com.databricks.spark.avro        |      5000000 |         0 |     109.21 |    102.159 |     21.810 |     24.300 |     23.338 |
| parquet                          |      5000000 |         0 |      29.45 |     73.579 |      1.807 |      2.784 |      2.707 |
| com.databricks.spark.csv         |      5200000 |         0 |     679.22 |     74.835 |     33.360 |     31.983 |     32.948 |
| com.databricks.spark.avro        |      5200000 |         0 |     113.58 |    107.995 |     23.478 |     24.067 |     23.951 |
| parquet                          |      5200000 |         0 |      30.63 |     76.790 |      2.479 |      3.363 |      3.055 |
| com.databricks.spark.csv         |      5400000 |         0 |     705.34 |     89.889 |     39.130 |     33.771 |     35.134 |
| com.databricks.spark.avro        |      5400000 |         0 |     117.95 |    112.262 |     24.361 |     24.967 |     25.463 |
| parquet                          |      5400000 |         0 |      31.81 |     82.223 |      2.690 |      3.434 |      3.212 |
| com.databricks.spark.csv         |      5600000 |         0 |     731.46 |     76.449 |     33.027 |     31.703 |     32.733 |
| com.databricks.spark.avro        |      5600000 |         0 |     122.32 |    116.216 |     24.905 |     25.223 |     25.771 |
| parquet                          |      5600000 |         0 |      32.99 |     80.443 |      2.677 |      3.064 |      3.017 |
| com.databricks.spark.csv         |      5800000 |         0 |     757.59 |     80.080 |     35.269 |     32.875 |     33.895 |
| com.databricks.spark.avro        |      5800000 |         0 |     126.69 |    114.121 |     24.588 |     25.506 |     27.338 |
| parquet                          |      5800000 |         0 |      34.16 |     84.486 |      3.204 |      3.530 |      3.416 |
| com.databricks.spark.csv         |      6000000 |         0 |     783.71 |     80.884 |     35.779 |     33.830 |     34.137 |
| com.databricks.spark.avro        |      6000000 |         0 |     131.05 |    121.414 |     26.558 |     28.497 |     28.081 |
| parquet                          |      6000000 |         0 |      35.34 |    105.610 |      4.558 |      6.016 |      5.043 |
| com.databricks.spark.csv         |      6200000 |         0 |     809.84 |     85.829 |     37.832 |     36.000 |     36.229 |
| com.databricks.spark.avro        |      6200000 |         0 |     135.42 |    126.251 |     27.744 |     28.943 |     29.800 |
| parquet                          |      6200000 |         0 |      36.52 |    156.386 |      5.989 |      5.007 |      5.852 |
| com.databricks.spark.csv         |      6400000 |         0 |     835.96 |     92.666 |     40.704 |     39.113 |     39.072 |
| com.databricks.spark.avro        |      6400000 |         0 |     139.79 |    139.344 |     29.197 |     30.985 |     32.481 |
| parquet                          |      6400000 |         0 |      37.69 |    195.288 |      5.221 |      6.473 |      7.790 |
| com.databricks.spark.csv         |      6600000 |         0 |     862.08 |     95.266 |     41.810 |     40.689 |     40.840 |
| com.databricks.spark.avro        |      6600000 |         0 |     144.16 |    146.077 |     34.514 |     33.177 |     33.596 |
| parquet                          |      6600000 |         0 |      38.87 |    216.942 |      3.823 |      4.213 |      3.859 |
| com.databricks.spark.csv         |      6800000 |         0 |     888.21 |    100.594 |     43.444 |     42.481 |     42.142 |
| com.databricks.spark.avro        |      6800000 |         0 |     148.53 |    151.261 |     32.914 |     37.818 |     44.639 |
| parquet                          |      6800000 |         0 |      40.05 |    103.173 |      2.499 |      4.744 |      3.841 |
| com.databricks.spark.csv         |      7000000 |         0 |     914.33 |     99.599 |     43.443 |     42.312 |     41.956 |
| com.databricks.spark.avro        |      7000000 |         0 |     152.89 |    156.552 |     37.649 |     41.106 |     42.770 |
| parquet                          |      7000000 |         0 |      41.23 |    104.054 |      2.264 |      4.108 |      3.489 |
| com.databricks.spark.csv         |      7200000 |         0 |     940.45 |    106.083 |     46.085 |     44.360 |     44.322 |
| com.databricks.spark.avro        |      7200000 |         0 |     157.27 |    161.589 |     39.109 |     42.679 |     38.072 |
| parquet                          |      7200000 |         0 |      42.41 |    108.318 |      2.539 |      4.247 |      4.156 |
| com.databricks.spark.csv         |      7400000 |         0 |     966.58 |    107.147 |     46.807 |     45.760 |     45.507 |
| com.databricks.spark.avro        |      7400000 |         0 |     161.63 |    211.741 |     38.721 |     38.889 |     38.131 |
| parquet                          |      7400000 |         0 |      43.58 |    141.250 |      3.534 |      5.515 |      5.452 |
| com.databricks.spark.csv         |      7600000 |         0 |     992.70 |    217.444 |     98.253 |     95.023 |     94.122 |
| com.databricks.spark.avro        |      7600000 |         0 |     166.00 |    359.579 |     61.241 |     67.100 |     66.368 |
| parquet                          |      7600000 |         0 |      44.77 |    194.045 |      5.562 |      7.838 |      7.965 |
| com.databricks.spark.csv         |      7800000 |         0 |    1018.83 |    195.767 |     91.644 |     88.445 |     88.166 |
| com.databricks.spark.avro        |      7800000 |         0 |     170.37 |    272.670 |     55.080 |     60.729 |     60.093 |
| parquet                          |      7800000 |         0 |      45.94 |    204.383 |      5.460 |      7.198 |      7.469 |
| com.databricks.spark.csv         |      8000000 |         0 |    1044.95 |    230.542 |    106.643 |    103.261 |    103.417 |
| com.databricks.spark.avro        |      8000000 |         0 |     174.74 |    299.648 |     64.709 |     68.059 |     67.212 |
| parquet                          |      8000000 |         0 |      47.12 |    216.999 |      5.129 |      8.296 |      8.257 |

Legend:
|Read 0 | SELECT COUNT(*) |
|Read 1 | SELECT COUNT(*) FROM SELECT * FROM data_frame WHERE C41 = 'pod.' AND C7 = 1 AND C8 = 0 AND C31 < 100 AND C35 > 0.1 AND C35 < 0.5 |
|Read 2 | SELECT COUNT(*) FROM SELECT "C1", "C2", "C3", "C7", "C8", "C31", "C35", "C41" FROM data_frame WHERE C41 = 'pod.' AND C7 = 1 AND C8 = 0 AND C31 < 100 AND C35 > 0.1 AND C35 < 0.5 |
      