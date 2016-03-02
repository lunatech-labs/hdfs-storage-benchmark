| Serializer                       | Records      | Run #     | Size [MB]  | Write [s]  | Read0 [s]  | Read 1 [s] | Read 2 [s] |
| ----------                       | ------:      | ----:     | --------:  | --------:  | --------:  | ---------: | ---------: |
| com.databricks.spark.csv         |       100000 |         0 |      13.16 |      0.770 |      0.395 |      0.517 |      0.430 |
| com.databricks.spark.avro        |       100000 |         0 |       2.20 |      1.179 |      0.545 |      0.444 |      0.301 |
| parquet                          |       100000 |         0 |       0.61 |      1.039 |      0.154 |      0.241 |      0.202 |
| com.databricks.spark.csv         |       200000 |         0 |      26.33 |      0.981 |      0.615 |      0.622 |      0.620 |
| com.databricks.spark.avro        |       200000 |         0 |       4.41 |      1.420 |      0.404 |      0.461 |      0.445 |
| parquet                          |       200000 |         0 |       1.21 |      1.108 |      0.151 |      0.180 |      0.170 |
| com.databricks.spark.csv         |       300000 |         0 |      39.49 |      1.373 |      0.850 |      0.860 |      0.829 |
| com.databricks.spark.avro        |       300000 |         0 |       6.61 |      2.018 |      0.542 |      0.624 |      0.605 |
| parquet                          |       300000 |         0 |       1.81 |      1.706 |      0.170 |      0.208 |      0.216 |
| com.databricks.spark.csv         |       400000 |         0 |      52.66 |      1.756 |      1.081 |      1.058 |      1.076 |
| com.databricks.spark.avro        |       400000 |         0 |       8.82 |      2.589 |      0.721 |      0.772 |      0.774 |
| parquet                          |       400000 |         0 |       2.42 |      2.211 |      0.206 |      0.246 |      0.246 |
| com.databricks.spark.csv         |       500000 |         0 |      65.82 |      2.180 |      1.305 |      1.327 |      1.335 |
| com.databricks.spark.avro        |       500000 |         0 |      11.02 |      3.262 |      0.860 |      0.916 |      0.934 |
| parquet                          |       500000 |         0 |       3.02 |      2.753 |      0.227 |      0.273 |      0.279 |
| com.databricks.spark.csv         |       600000 |         0 |      78.98 |      2.552 |      1.530 |      1.552 |      1.521 |
| com.databricks.spark.avro        |       600000 |         0 |      13.22 |      3.782 |      1.029 |      1.087 |      1.131 |
| parquet                          |       600000 |         0 |       3.63 |      3.226 |      0.255 |      0.305 |      0.312 |
| com.databricks.spark.csv         |       700000 |         0 |      92.15 |      3.012 |      1.797 |      1.792 |      1.791 |
| com.databricks.spark.avro        |       700000 |         0 |      15.43 |      4.483 |      1.215 |      1.291 |      1.311 |
| parquet                          |       700000 |         0 |       4.23 |      3.816 |      0.278 |      0.340 |      0.329 |
| com.databricks.spark.csv         |       800000 |         0 |     105.31 |      3.396 |      2.042 |      2.011 |      2.011 |
| com.databricks.spark.avro        |       800000 |         0 |      17.63 |      5.012 |      1.350 |      1.446 |      1.447 |
| parquet                          |       800000 |         0 |       4.83 |      4.439 |      0.322 |      0.364 |      0.390 |
| com.databricks.spark.csv         |       900000 |         0 |     118.48 |      3.777 |      2.256 |      2.308 |      2.262 |
| com.databricks.spark.avro        |       900000 |         0 |      19.84 |      5.756 |      1.504 |      1.620 |      1.623 |
| parquet                          |       900000 |         0 |       5.44 |      4.900 |      0.340 |      0.419 |      0.393 |
| com.databricks.spark.csv         |      1000000 |         0 |     131.64 |      4.214 |      2.522 |      2.494 |      2.493 |
| com.databricks.spark.avro        |      1000000 |         0 |      22.04 |      6.283 |      1.684 |      1.783 |      1.781 |
| parquet                          |      1000000 |         0 |       6.04 |      5.361 |      0.383 |      0.442 |      0.432 |

Legend:
|Read 0 | SELECT COUNT(*) |
|Read 1 | SELECT COUNT(*) FROM SELECT * FROM data_frame WHERE C41 = 'pod.' AND C7 = 1 AND C8 = 0 AND C31 < 100 AND C35 > 0.1 AND C35 < 0.5 |
|Read 2 | SELECT COUNT(*) FROM SELECT "C1", "C2", "C3", "C7", "C8", "C31", "C35", "C41" FROM data_frame WHERE C41 = 'pod.' AND C7 = 1 AND C8 = 0 AND C31 < 100 AND C35 > 0.1 AND C35 < 0.5 |
      
