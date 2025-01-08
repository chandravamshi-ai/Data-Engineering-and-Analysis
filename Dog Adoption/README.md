
# 🐾 Dog Adoption Data Engineering Project

A data engineering project designed to demonstrate the process of creating and managing a PostgreSQL database, ingesting and transforming raw CSV data, and structuring it for analysis. This project is a perfect showcase of skills in data pipeline development and database management.

---

## 📝 Project Overview

The goal of this project is to:

![etl](https://github.com/chandravamshi-ai/Data-Engineering-and-Analysis/blob/main/Dog%20Adoption/etl%20img.png)
1. Create a PostgreSQL database named `dog_adoption`.
2. Design and populate tables for handling dog adoption data from kaggle dog [datasets](https://www.kaggle.com/datasets/whenamancodes/dog-adoption).
3. Transform raw data into clean(remove nan rows) - other than this data is clean, structured tables.
4. Demonstrate data engineering skills, including ETL, database management, and code modularity.

---

## 🛠️ Features

- **Database Management**: Automatically creates and connects to a PostgreSQL database.
- **Table Design**: Defines schema for `description`, `travel`, and `location` tables.
- **Data Ingestion**: Loads data from CSV files into the database.
- **Modular Code**: Follows clean coding practices, using functions and environment variables for configuration.

---

## 📂 Project Structure

```plaintext
├── Datasets/                     # Folder containing CSV files
│   ├── description.csv       # Dog breed details
│   ├── travel.csv            # Travel-related data
│   └── location.csv          # Location data
├── main.py                   # Main script to run the project
├── database_handler.py       # script to managing database (modular, encapuslated code)
├── table_manager.py          # script to managing table create, ingesting data to tables (modular, encapuslated code)
├── check_connection.py       # script to check connection to postgres database
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables for database credentials
└── README.md                 # Project documentation
```

---

## 🚀 How to Run the Project

### 1️⃣ Prerequisites

- **PostgreSQL** installed and running locally.
- Python 3.8 or higher installed.
- Install project dependencies using `pip`:
  ```bash
  pip install -r requirements.txt
  ```

### 2️⃣ Setup

1. Create a `.env` file in the root directory with the following content:
   ```env
   DB_HOST=127.0.0.1
   DB_NAME=postgres
   DB_USER=your_postgres_user
   DB_PASSWORD=your_postgres_password
   ```

### 3️⃣ Run the Project

Execute the main script:
```bash
python main.py
```

### 4️⃣ Expected Outputs

- A PostgreSQL database named `dog_adoption` will be created.
- Three tables (`description`, `travel`, and `location`) will be created and populated with data from the CSV files.
- Logs will confirm successful execution of each step.

---

## 📊 Data Overview

### `description.csv`
Details about the dogs, including:
- Primary and secondary breeds
- Color, age, sex, size, coat type
- Contact information (city, state, zip)

### `travel.csv`
Travel-related data, including:
- City and state
- Statuses like "found" or "still there"

### `location.csv`
Insights about dog movement, including:
- Exported and imported counts
- Total movements
- Whether the location is within the US

---

## 🛠️ Tech Stack

- **Database**: PostgreSQL
- **Languages**: Python
- **Libraries**:
  - `psycopg2` for database connection and queries
  - `pandas` for CSV file handling and data manipulation
  - `dotenv` for secure environment variable handling
  - `logging` for tracking project execution

---

## 📚 Key Learnings

This project demonstrates the following skills:
- Setting up and managing PostgreSQL databases.
- Automating database table creation and data ingestion pipelines.
- Writing clean, modular, and reusable Python code.
- Using `.env` for secure configuration management.
- Working with large datasets using `pandas`.

---

## 💡 Future Improvements

- Add data validation to ensure data integrity before ingestion.
- Automate the process for larger datasets or streaming data.
- Create dashboards for visualizing dog adoption trends.
