# Movie Ratings Analysis

This project analyzes movie ratings by genre and year, considering only movies released after 1989 and ratings from users aged 18-49. The analysis is performed using PySpark on Databricks Community Edition.

## How to Run

1. Clone the repository.
2. Import the `.dbc` file into your Databricks workspace.
3. Upload the `ratings.dat`, `users.dat`, and `movies.dat` files to DBFS.
4. Run the notebook.

## Results

The final results include the average ratings per genre and year, which are saved in a CSV file.

## Requirements

- Databricks Community Edition
- Apache Spark 3.x
- Python 3.x
