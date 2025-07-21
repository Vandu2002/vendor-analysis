import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


def create_vendor_summary(conn):
    '''This function merges tables to produce a vendor summary with additional computed columns.'''

    query = """
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS Freight_Cost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),    

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS Actual_Price,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),

    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(Sales_Price) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )

    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.Actual_Price,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.Freight_Cost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    """

    try:
        vendor_sales_summary = pd.read_sql_query(query, conn)
        logging.info("Vendor summary successfully created.")
        return vendor_sales_summary

    except Exception as e:
        logging.error("Error while creating vendor summary: %s", e)
        raise


def clean_data(df):
    '''This function cleans the vendor summary DataFrame.'''

    # Changing datatype to float
    df['Volume'] = df['Volume'].astype(float)

    # Filling missing values with 0
    df.fillna(0, inplace=True)

    # Removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Creating new columns for better analysis
    df['Gross_Profit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['Gross_Profit'] / df['TotalSalesDollars'].replace(0, 1)) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, 1)
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars'].replace(0, 1)

    return df
    
if __name__ == '__main__':
    # Creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summary(conn)
    logging.info("\n" + str(summary_df.head()))

    logging.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logging.info("\n" + str(clean_df.head()))

    logging.info('Ingesting data...')
    # Make sure you have defined this function:
    # def ingest_db(df, table_name, conn): df.to_sql(...)
    ingest_db(clean_df, 'vendor_sales_summary', conn)

    logging.info('Completed.')
