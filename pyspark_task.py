from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

def get_product_category_pairs(products_df, categories_df):
    # Объединяем датафреймы по полю product_id (или другому полю, которое их связывает)
    merged_df = products_df.join(categories_df, products_df["product_id"] == categories_df["product_id"], "left_outer")

    # Выбираем только нужные колонки
    result_df = merged_df.select(products_df["product_name"], categories_df["category_name"])

    # Создаем датафрейм с продуктами, у которых нет категорий
    products_without_categories_df = products_df.join(categories_df, products_df["product_id"] == categories_df["product_id"], "left_anti")
    products_without_categories_df = products_without_categories_df.select(products_df["product_name"], when(col("category_name").isNull(), lit("No category")).alias("category_name"))

    # Объединяем датафреймы с парами "Имя продукта – Имя категории" и продуктами без категорий
    result_df = result_df.union(products_without_categories_df)

    return result_df

# Пример использования
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ProductCategoryPairs") \
        .getOrCreate()

    # Примеры данных
    products_data = [("product1", 1), ("product2", 2), ("product3", 3)]
    categories_data = [("category1", 1), ("category2", 2)]

    # Создаем датафреймы
    products_df = spark.createDataFrame(products_data, ["product_name", "product_id"])
    categories_df = spark.createDataFrame(categories_data, ["category_name", "product_id"])

    # Получаем пары "Имя продукта – Имя категории" и имена продуктов без категорий
    result_df = get_product_category_pairs(products_df, categories_df)
    result_df.show()

    spark.stop()
