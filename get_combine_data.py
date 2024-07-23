from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def combine_products_and_categories(df_1, df_2):
    """
    Комбинирует информацию о продуктах и их категориях, 
    включая продукты без категорий.
    """
    
    combined_df = df_1.union(df_2)
    filtered_df = combined_df.select("ProductName", "Category")

    return filtered_df


if __name__ == '__main__':
    # создаём сессию   
    spark = SparkSession.builder.appName("Shop_app").getOrCreate()

    # DataFrame с продуктами и их категориями
    data1 = [
        ("ProductA", "Category1"), 
        ("ProductB", "Category2"), 
        ("ProductC", None)]
    df1 = spark.createDataFrame(data=data1, schema=["ProductName", "Category"])

    # DataFrame с продуктами без категорий
    data2 = [("ProductD",), ("ProductE",)]
    df2 = spark.createDataFrame(data=data2, schema=["ProductName"])
    
    # # Добавляем столбец Category со значением None
    df2 = df2.withColumn("Category", lit(None))

    combined_df = combine_products_and_categories( 
        df_1=df1, 
        df_2=df2
    )
    
    combined_df.show()
