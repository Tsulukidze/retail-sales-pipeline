"""Description of the source file, kept in one place.

Maps each column name in the Kaggle CSV to the snake_case name we use in the
warehouse. File validation and the staging load both use this mapping.
"""

SOURCE_TO_STAGING_COLUMNS: dict[str, str] = {
    "CustomerID": "customer_id",
    "ProductID": "product_code",
    "Quantity": "quantity",
    "Price": "unit_price",
    "TransactionDate": "transaction_ts",
    "PaymentMethod": "payment_method",
    "StoreLocation": "store_location",
    "ProductCategory": "product_category",
    "DiscountApplied(%)": "discount_pct",
    "TotalAmount": "total_amount",
}

EXPECTED_SOURCE_COLUMNS: tuple[str, ...] = tuple(SOURCE_TO_STAGING_COLUMNS)

SOURCE_DATE_FORMAT = "%m/%d/%Y %H:%M"
