from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime
from pyspark.sql import functions as g
from pyspark.sql import types




def explore_data(df):
    """
        tell us the missing data.
        :param df: dataframe to process
    """    
    
    for column in df.columns:
        missing = df.filter(df[column].isNull()).count()
        missing_percentage = round((100*(missing/df.count())),3)
        print(f"{missing} is missing in {column}, {missing_percentage}%")


def write_to_parquet(df, output_path, table_name):
    """
        Writes the dataframe as parquet file.
        :param df: spark dataframe to write
        :param output_path: output path where to write
        :param table_name: name of the table
    """
    
    file_path = f"{output_path}{ table_name}"
    print(f"Writing table {table_name} to {file_path}")
    df.write.mode("overwrite").parquet(file_path)
    print("Write complete!")

    
def clean_null_values(df):
    """
        drop columns with 90% null values and drop duplicates
        :param df: dataframe 
    """
    columns_todrop =[]
    number_befor = df.count()
    
    for column in df.columns:
        missing=df.filter(df[column].isNull()).count()
        missing_percentage=round((100*(missing/df.count())),2)
        if missing_percentage >= 90:
            columns_todrop.append(column)
            
    df = df.drop(*columns_todrop) 
    print(f"droped columns {columns_todrop}")
    print("cleaning complete")
    df = df.dropna(how='all')
    df = df.drop_duplicates()
    number_after = df.count()
    
    print(f"no of rows befor cleaning: {number_befor} ,no of rows after cleaning: {number_after}")
    return df


def perform_quality_check(df, table_name):
    """Check data completeness by ensuring there are records in each table.
        :param df: dataframe to check counts on.
        :param table_name: name of table
    """
    
    record_count = df.count()

    if (record_count == 0):
        print(f"Data quality check failed for {table_name} with zero records!")
    else:
        print(f"Data quality check passed for {table_name} with record_count: {record_count} records.")
        

#create tables
def process_immigration(cleaned_df,output_dir_path):
    """Create immigration fact table."""
    
    table_name = "/fact_immigration"
    final_data = cleaned_df.select(col("cicid").cast('int').alias("record_id"),
                    col("i94yr").cast("int").alias("year"),
                    col("i94mon").cast("int").alias("month"),
                    col("i94cit").cast("int").alias("birth_country"),
                    col("i94res").cast("int").alias("resdence_country"),
                    col("depdate"),
                    col("arrdate"),
                    col("i94port").cast("string").alias("admission_port_code"),
                    col("i94mode").cast("int").alias("arrival_mode"),
                    col("i94addr").cast("string").alias("arrival_state_code"),
                    col("biryear").cast("int").alias("applicant_birth_year"),
                    col("gender").cast("string"),
                    col("i94bir").cast("int").alias("applicant_age"),
                    col("fltno").cast("string").alias("flight_number"),
                    col("airline"),
                    col("dtaddto")).withColumn("limit",from_unixtime("dtaddto").cast("timestamp")).\
                    withColumn("departure_date", from_unixtime("depdate").cast("timestamp")).\
                    withColumn("arrival_date",from_unixtime("arrdate").cast("timestamp")).drop("dtaddto","depdate","arrdate").\
                    withColumn('visa_id', g.monotonically_increasing_id())
    
    
    print(f"Data for {table_name} table was successfully processed.")
    write_to_parquet(final_data, output_dir_path, table_name)
    


def process_demographic(cleaned_df,output_dir_path):
    """Create demographic dimension table."""
    
    table_name = "dim_demographic"
    cleaned_df = cleaned_df.select(["State Code", "State", "Median Age", "Male Population", "Female Population", "Total Population",
                              "Number of Veterans","Average Household Size","Foreign-born"])

    demog_df = cleaned_df.groupBy(col("State Code").alias("state_code"),col("State")).agg(
        g.sum("Total Population").cast("int").alias("total_population"),
        g.sum("Male Population").cast("int").alias("male_population"),
        g.sum("Female Population").cast("int").alias("female_population"),
        g.sum("Number of Veterans").cast("int").alias("number_of_veterans"),
        g.sum("Foreign-born").cast("int").alias("foregin_born"),
        g.avg("Median Age").cast(types.DoubleType()).alias("median_age"),
        g.avg("Average Household Size").cast(types.DoubleType()).alias("average_household_size"))
    
    print(f"Data for {table_name} table was successfully processed.")
    write_to_parquet(demog_df, output_dir_path, table_name)
    
    
    
def process_Temperature(cleaned_df,output_dir_path):
    """Create Temperature dimension table."""
   
    table_name = "dim_temprature"
    Temperature_df = cleaned_df.withColumn("year", g.year("dt")).withColumn("month", g.month("dt"))
    Temperature_df = Temperature_df.groupBy([col("Country").alias("country"), "year", "month"]).agg(
             g.avg("AverageTemperature").alias("average_temperature"),
             g.avg("AverageTemperatureUncertainty").alias("average_temperature_uncertainty")).\
             dropna()
    
    print(f"Data for {table_name} table was successfully processed.")
    write_to_parquet(Temperature_df, output_dir_path, table_name)


    
def process_airport(cleaned_df,output_dir_path):
    """Create airport dimension table."""
        
    table_name = "dim_airport"
    airport_df = cleaned_df.select(["ident", "type", "iata_code", "name", "iso_country", "iso_region", "municipality", "gps_code",                      "coordinates","elevation_ft"]).dropDuplicates(["ident"]) 
    
    
    print(f"Data for {table_name} table was successfully processed.")
    write_to_parquet(airport_df, output_dir_path, table_name)

    
    
def process_visa(cleaned_df,output_dir_path):
    """create visa dimension"""
    table_name = "dim_visa"
    visa_df = cleaned_df.withColumn("visa_id", g.monotonically_increasing_id()) \
                .select(["visa_id","i94visa", "visatype", "visapost"]) \
                .dropDuplicates(["i94visa", "visatype", "visapost"]) 
    
    
    print(f"Data for {table_name} table was successfully processed.")    
    write_to_parquet(visa_df, output_dir_path, table_name)
    
    

    
    
def process_applicant_nationality(cleaned_df,output_dir_path):
    """create applicant_nationality dimension"""
        
    table_name = "dim_applicant_nationality"
    applicant_nationality = cleaned_df .dropDuplicates()
    applicant_nationality = applicant_nationality.select(col("birth_country").cast("int"),col("country").cast("string"))
     
        
    print(f"Data for {table_name} table was successfully processed.")    
    write_to_parquet(applicant_nationality, output_dir_path, table_name)

    
    
def process_addmission_port(cleaned_df,output_dir_path):
    """create addmission_port dimension"""
        
    table_name = "dim_addmission_port"
    addmission_port = cleaned_df .dropDuplicates()
    addmission_port = addmission_port.select(col("admission_port_code"),col("country"))
     
    print(f"Data for {table_name} table was successfully processed.")    
    write_to_parquet(addmission_port, output_dir_path, table_name)
    