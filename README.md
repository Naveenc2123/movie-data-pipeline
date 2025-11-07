🎬 Movie Data Pipeline
======================

📘 Overview
-----------

This project implements a simple **ETL (Extract, Transform, Load)** data pipeline that ingests movie data from multiple sources, enriches it with external API data, and loads it into a relational database for analysis.

The pipeline processes:

*   **Local CSV files** from the [MovieLens dataset](https://grouplens.org/datasets/movielens/latest/)
    
*   **External API data** from the [OMDb API](http://www.omdbapi.com/)
    

It then stores the combined, cleaned data into a structured database (SQLite) and answers analytical questions through SQL queries.

⚙️ Project Structure
--------------------

```bash
movie-data-pipeline/
├── etl.py            # Python script for ETL process
├── schema.sql        # SQL schema for database tables
├── queries.sql       # Analytical SQL queries
├── requirements.txt  # Python dependencies
└── README.md         # Project documentation `
```
🧩 Requirements

**Languages / Libraries**

*   Python 3.9+
    
*   pandas
    
*   requests
    
*   sqlalchemy
    
*   sqlite3 (built-in)
    

Install dependencies:

```bash
pip install -r requirements.txt   `
```
**External API**

*   OMDb API key — get it from: http://www.omdbapi.com/apikey.aspx
    

Store it in a .env file or directly in your script:

```bash
OMDB_API_KEY=your_api_key_here   `
```
🛠️ Setup Instructions

### 1\. Database Setup

Run the SQL schema to create tables:

```bash
sqlite3 movies.db < schema.sql
 ```

### 2\. Run the ETL Pipeline

```bash
python etl.py
 ```

This script will:

*   Extract movie and rating data from movies.csv and ratings.csv
    
*   Call the OMDb API to get director, plot, and box office info
    
*   Clean and merge all data
    
*   Load the final dataset into your SQLite database
    

### 3\. Run Analytical Queries

Once data is loaded, open your database in SQLite and execute:

```bash
.read queries.sql
 ```

🧠 Database Design

**Tables:**

1.  movies — stores movie details (title, genres, year, director, etc.)
    
2.  ratings — stores user ratings linked to movies
    
3.  genres — stores genres in a normalized format (many-to-many via movie\_genres)
    
4.  movie\_genres — relationship table between movies and genres
    

**Relationships:**

*   One movie → many ratings
    
*   One movie → many genres
    

🧮 Example Queries

1.  **Highest average rated movie**
    
2.  **Top 5 genres by average rating**
    
3.  **Director with the most movies**
    
4.  **Average rating per release year**
    

These queries are included in queries.sql.

💡 Design Choices
-----------------

*   **SQLite** chosen for simplicity; can easily switch to MySQL/PostgreSQL via SQLAlchemy.
    
*   **pandas** used for data cleaning and merging due to flexibility and speed.
    
*   **OMDb API** used to enrich data with metadata (Director, BoxOffice, Plot).
    
*   ETL designed to be **idempotent** — re-running won’t duplicate entries.
    

🧾 Author

**Naveen C | Email : naveen22442123@gmail.com**
