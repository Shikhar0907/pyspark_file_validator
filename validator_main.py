from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def read_files(source_path, comparable_path):
    """
    This is a method which will read the files.
    Args:
        source_path(str): Source file path
        comparable_path(str): Comparable file path
    Return:
    """
    source_df = spark.read.csv(source_path, header=True)
    comparable_df = spark.read.csv(comparable_path, header=True)

    return source_df, comparable_df


def compare_file_name(source_path, comparable_path):
    """This method will check the file names.
    Args:
        source_path(str): source file path
        comparable_path(str): comparable file path
    Returns:
        Boolean
    """
    source_file_name = source_path.split('/')[-1]
    comparable_file_name = comparable_path.split('/')[-1]
    if source_file_name == comparable_file_name:
        return True
    else:
        return False


def compare_files(source_df, comparable_df, select_columns):
    """
    This method will compare the source and comparable file and
    return if any difference is there.
    Args:
        source_path(DataFrame): Source data frame
        comparable_path(DataFrame): Comparable data frame
    Return:
        Boolean
    """
    diff_df = source_df.subtract(comparable_df)
    if diff_df.rdd.isEmpty():
        return True
    else:
        print(diff_df.show())
        return False


def check_column_number(source_df, comparable_df):
    """
    This method is for checking if the column names match or not.
    Args:
        source_df(DataFrame): Source Data frame
        comparable_df(DataFrame): Comparable Data frame
    Return:
        Boolean
    """
    source_columns = source_df.columns
    comparable_columns = comparable_df.columns
    unmatched_columns = []
    if_matched_columns = True
    if len(source_columns) == len(comparable_columns):
        for columns in source_columns:
            if columns not in comparable_columns:
                unmatched_columns.append(columns)
                if_matched_columns = False
    return if_matched_columns


def comparator_main(source_path, comparable_path, sort_columns=None):

    if not compare_file_name(source_path, comparable_path):
        return "File name is not same."

    source_df, comparable_df = read_files(source_path, comparable_path)

    if not check_column_number(source_df, comparable_df):
        return "Columns not matching!!"

    if sort_columns:
        source_df = source_df.sort(sort_columns)
        comparable_df = comparable_df.sort(sort_columns)

    if not compare_files(source_df, comparable_df, sort_columns):
        return "Files content not same"

    return "File 100% matched."


if __name__ == '__main__':
    source_path = 'C://Users/Shikhar0907/Desktop/file_comparator/Glue/Test_1.csv'
    comparable_path = 'C://Users/Shikhar0907/Desktop/file_comparator/RDS/Test_1.csv'
    print(comparator_main(source_path, comparable_path, sort_columns=['Column1']))

