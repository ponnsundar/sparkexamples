# Spark Workshop Exercises for Databricks

Independent PySpark applications based on exercises from
[Jacek Laskowski's Spark Workshop](https://jaceklaskowski.github.io/spark-workshop/exercises/).

Each file is a self-contained Databricks notebook. Upload to Databricks and run directly.

## Exercises

### Spark SQL
1. `01_split_variable_delimiter.py` — Split with variable delimiter per row
2. `02_most_important_rows_priority.py` — Select most important rows per priority
3. `03_adding_count_to_source.py` — Add count to source DataFrame
4. `04_limiting_collect_set.py` — Limiting collect_set standard function
5. `05_structs_column_names_values.py` — Structs for column names and values
6. `06_merge_two_rows.py` — Merging two rows
7. `07_exploding_structs_array.py` — Exploding structs array
8. `08_display_spark_sql_version.py` — Standalone app to display Spark SQL version
9. `09_using_csv_data_source.py` — Using CSV Data Source
10. `10_finding_ids_word_in_array.py` — Finding IDs of rows with word in array column
11. `11_dataset_flatmap.py` — Using Dataset.flatMap operator
12. `12_reverse_engineering_show.py` — Reverse-engineering Dataset.show output
13. `13_flattening_array_columns.py` — Flattening array columns
14. `14_most_populated_cities.py` — Finding most populated cities per country
15. `15_upper_standard_function.py` — Using upper standard function
16. `16_explode_standard_function.py` — Using explode standard function
17. `17_difference_days_dates.py` — Difference in days between dates as strings
18. `18_counting_years_months.py` — Counting occurrences of years and months
19. `19_null_fields_schema.py` — Why are all fields null when querying with schema?
20. `20_add_days_to_date.py` — How to add days to date
21. `21_using_udfs.py` — Using UDFs
22. `22_calculating_aggregations.py` — Calculating aggregations
23. `23_max_values_per_group.py` — Finding maximum values per group
24. `24_collect_values_per_group.py` — Collect values per group
25. `25_multiple_aggregations.py` — Multiple aggregations
26. `26_pivot_single_row_matrix.py` — Using pivot for single-row matrix
27. `27_pivot_cost_avg_collect.py` — Using pivot for cost average and collecting values
28. `28_pivoting_multiple_columns.py` — Pivoting on multiple columns
29. `29_exam_assessment_report.py` — Generating exam assessment report
30. `30_long_to_wide_format.py` — Flattening dataset from long to wide format
31. `31_bestsellers_per_genre.py` — Finding 1st and 2nd bestsellers per genre
32. `32_salary_gap_per_dept.py` — Gap between current and highest salaries per dept
33. `33_running_total.py` — Calculating running total / cumulative sum
34. `34_diff_consecutive_rows.py` — Difference between consecutive rows per window
35. `35_arrays_to_string.py` — Converting arrays of strings to string
36. `36_percent_rank.py` — Calculating percent rank
37. `37_first_non_null_per_group.py` — Finding first non-null value per group
38. `38_longest_sequence.py` — Finding longest sequence (window aggregation)
39. `39_most_common_prefix.py` — Finding most common non-null prefix per group
40. `40_rollup_salaries.py` — Using rollup for total and average salaries

### Spark Structured Streaming
41. `41_first_streaming_app.py` — First standalone structured streaming application
42. `42_streaming_csv.py` — Streaming CSV datasets

### Spark MLlib
43. `43_email_classification.py` — Email classification
