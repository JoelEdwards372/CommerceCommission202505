class JoelsETL:
    """
    A class containing ETL functions for data processing with PySpark.
    """

    def __init__(self, spark: SparkSession):
        """
        Initializes the JoelsETL class with a SparkSession.

        Args:
            spark (SparkSession): The SparkSession to use for data processing.
        """
        self.spark = spark
		
		
		
	# Function that explicitly TRANSFORMS string type fields that are either date, integer, or double type
	def fn_transform_cast(df, columns, cast_type):
		"""
		Objective:
			Casts multiple string columns to a specified data type in a PySpark DataFrame.

		Args:
			df (DataFrame): The input PySpark DataFrame.
			columns (list of str): A list of column names to be cast.
			cast_type (str): The target data type for casting ("date", "integer", or "double").

		Returns:
			DataFrame: A new DataFrame with specified columns cast to the target type.

		Use Case:
		i.e. fn_transform_cast(df, ["col1", "col2"], "integer")
		i.e. fn_transform_cast(df, ["col3"], "date")
		"""
		valid_cast_types = ["date", "integer", "double"]
		if cast_type.lower() not in valid_cast_types:
			raise ValueError(f"Invalid cast_type. Supported types are: {valid_cast_types}")

		for column in columns:
			new_col_name = f"tfm_{column}"
			if cast_type.lower() == "date":
				df = df.withColumn(new_col_name, col(column).cast("date"))
			else:
				# For integer and double, remove commas before casting
				df = df.withColumn(new_col_name, regexp_replace(col(column), ",", "").cast(cast_type))
		return df



	# Function to TRANSFORM tables into dimensional tables
	def fn_create_dim_table(df_input, dimension_cols, order_by_col=None):
		"""
		Objective:
			Creates a dimension table from a Spark DataFrame based on specified columns.

		Args:
			df_input (DataFrame): The input PySpark DataFrame with transformed columns.
			dimension_cols (list of str): A list of column names to be included
										 in the dimension table.
			order_by_col (str, optional): The name of the column to order the dimension
										  table by. If None, the first column in
										  dimension_cols is used for ordering.
										  Defaults to None.

		Returns:
			DataFrame: The created PySpark dimension table.

		Use Case (Creating Location Dimension):
		df_dim_location = create_dimension_table(
			df_spark_tfm,
			["tfm_Location Id", "Location"],
			"tfm_Location Id" # Optional: explicitly specify the order column
		)

		Use Case (Creating Period Dimension - basic):
		df_dim_period_basic = create_dimension_table(
			df_spark_tfm,
			["tfm_Time Frame"]
		)
		"""
		# Validate input columns
		if not isinstance(dimension_cols, list) or not dimension_cols:
			raise ValueError("dimension_cols must be a non-empty list of column names.")

		if not all(c in df_input.columns for c in dimension_cols):
			missing_cols = [c for c in dimension_cols if c not in df_input.columns]
			raise ValueError(f"Input DataFrame is missing required columns: {missing_cols}")

		# Determine the column for ordering
		if order_by_col is None:
			order_col = dimension_cols[0]
		else:
			if order_by_col not in dimension_cols:
				 raise ValueError(f"order_by_col '{order_by_col}' must be one of the columns in dimension_cols.")
			order_col = order_by_col

		# Create the dimension table
		df_dimension = (df_input.
						select(dimension_cols).
						distinct().
						orderBy(order_col))

		return df_dimension



	# Function that creates new dataframes, accepting user field selections
	def fn_dataframe_selections(df_spark, selected_fields):
	  """
	  Selects and reorders a subset of columns in a Spark DataFrame
	  according to a provided list.

	  Args:
		df_spark (DataFrame): The input PySpark DataFrame.
		selected_fields (list of str): A list of column names
											  specifying the subset of columns
											  to select and their desired order.
											  These column names should match
											  names present in the input DataFrame.

	  Returns:
		DataFrame: A new DataFrame with the specified columns in the desired order.
				   Returns None if selected_fields is empty or not a list.

	  Raises:
		  ValueError: If any column in desired_subset_order is not found
					  in the input DataFrame.

	  Use Cases (including testing):
		Example of how to use the function:
		Assuming df_spark is your Spark DataFrame

		Example 1: Select and reorder a few columns
		selected_fields = ["tfm_Time Frame", "Location", "tfm_Median Rent"]
		df_subset1 = fn_dataframe_selections(df_spark, selected_fields)
		if df_subset1 is not None:
			df_subset1.show()

		Example 2: Select and reorder almost all columns in a specific order
		selected_fields = ["tfm_Location Id", "Location", "tfm_Time Frame", "tfm_Lodged Bonds",
		"tfm_Active Bonds", "tfm_Closed Bonds", "tfm_Median Rent"]
		df_subset2 = fn_dataframe_selections(df_spark, selected_fields)
		if df_subset2 is not None:
			df_subset2.show()

		Example 3: Handling an invalid column (will raise ValueError)
		try:
			selected_fields = ["tfm_Time Frame", "NonExistentColumn"]
			df_invalid = fn_dataframe_selections(df_spark, selected_fields)
		except ValueError as e:
			print(f"Caught expected error: {e}")
	  """
	  if not isinstance(selected_fields, list) or not selected_fields:
		  print("Error: desired_subset_order must be a non-empty list of column names.")
		  return None

	  # Check if all requested columns exist in the DataFrame
	  missing_cols = [col_name for col_name in selected_fields if col_name not in df_spark.columns]
	  if missing_cols:
		  raise ValueError(f"The following requested columns are not found in the DataFrame: {missing_cols}")

	  # Select and reorder the columns
	  return df_spark.select(selected_fields)



	# Function that expands of data/time fields for dim date relevant tables
	def fn_add_period_attributes(df_spark, date_column):
	  """
	  Adds Year, Annual Quarter, and Financial Quarter attributes to a Spark DataFrame
	  based on a specified date column.

	  Args:
		df_spark (DataFrame): The input PySpark DataFrame.
		date_column (str): The name of the date column from which to calculate the attributes.
						   This column should be of a date or timestamp type.

	  Returns:
		DataFrame: A new DataFrame with the added period attributes.

	  Raises:
		ValueError: If the specified date_column is not found in the DataFrame
					or is not of a date/timestamp type.

	  Use Cases:
	  # Assuming df_dim_period is your Spark DataFrame and 'tfm_Time Frame' would be the date column
	  # df_dim_period_with_attributes = add_period_attributes(df_dim_period, "tfm_Time Frame")
	  """
	  if date_column not in df_spark.columns:
		  raise ValueError(f"The specified date column '{date_column}' is not found in the DataFrame.")

	  # Check if the column is a date or timestamp type
	  column_type = df_spark.schema[date_column].dataType
	  if not (isinstance(column_type, DateType) or isinstance(column_type, TimestampType)):
		   raise ValueError(f"The column '{date_column}' is not a date or timestamp type. "
							f"Current type is {column_type}.")

	  df_with_attributes = (df_spark.
					 withColumn("Year", year(col(date_column))).
					 withColumn("Annual Quarter", concat(year(col(date_column)), lit("Q"), quarter(col(date_column)))).
					 withColumn("Financial Quarter",
								concat(
									when(month(col(date_column)) >= 7, year(col(date_column)))
									.otherwise(year(col(date_column)) - 1),
									lit("Q"),
									when(month(col(date_column)) >= 7, ceil((month(col(date_column)) - 6) / 3))
									.otherwise(ceil((month(col(date_column)) + 6) / 3))
								)
					 ))
	  return df_with_attributes