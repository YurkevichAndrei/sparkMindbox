from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("sparkMindbox").getOrCreate()

# создание DataFrame Продукты
products_list = [(0, "Product0"), (1, "Product1"), (2, "Product2")]
products_schema = ["id", "name"]
products = spark.createDataFrame(products_list, products_schema)

# создание DataFrame Категории
categories_list = [(0, "Сategory0"), (1, "Сategory1"), (2, "Сategory2")]
categories_schema = ["id", "name"]
categories = spark.createDataFrame(categories_list, categories_schema)

# создание DataFrame Продукты-Категории
product_categories_list = [(0, 0, 0), (1, 0, 1), (2, 0, 2), (3, 1, 0), (4, 1, 1)]
product_categories_schema = ["id", "product_id", "category_id"]
products_categories = spark.createDataFrame(product_categories_list, product_categories_schema)


def get_name_products_and_name_categories():
    product_with_categories = products.join(
        products_categories,
        products["id"] == products_categories["product_id"],
        how="left"
    )

    result_product_with_categories = product_with_categories.join(
        categories,
        product_with_categories["category_id"] == categories["id"],
        how="left"
    )

    final_result = result_product_with_categories.select(
        products["name"].alias("product_name"),
        categories["name"].alias("category_name")
    )

    return final_result


result = get_name_products_and_name_categories()
result.show()
