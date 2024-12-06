# Data-Modeling-with-DBT-Handling-440K-Records (PostgreSQL Data Pipeline with DBT)

This project demonstrates how to set up a data pipeline with PostgreSQL in Docker, load and preprocess 440k records using Python, and process data models using DBT.

---

## 🚀 **Project Workflow**

```plaintext
+------------------+       +-------------------+       +------------------+
|   Docker Setup   | --->  | Data Preprocessing| --->  |   DBT Modeling   |
| PostgreSQL Setup |       | Python Script     |       | dbt run          |
+------------------+       +-------------------+       +------------------+
```

---

## 📋 **Setup Instructions**

### 1️⃣ **Set Up PostgreSQL in Docker**

Run the following command to create and start a PostgreSQL container:

```bash
docker run -d --name postgres-dbt-practice \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin \
  -e POSTGRES_DB=learndbt \
  -p 5432:5432 \
  postgres:latest
```

---

### 2️⃣ **Install Dependencies**

Ensure you have Python and pip installed. Then, install the required Python libraries:

```bash
pip install pandas sqlalchemy psycopg2 python-dotenv
```

---

### 3️⃣ **Load and Preprocess Data**

Use the provided Python script to preprocess and upload data to PostgreSQL:

```bash
python postgres_dataloader.py
```

#### **`postgres_dataloader.py` Overview**:
- **Reads** a compressed CSV file with 440k records.
- **Cleans and preprocesses** data (fills nulls, drops unnecessary columns, and ensures consistency).
- **Uploads** the processed data to the PostgreSQL database in a table named `hacker_data`.

---

### 4️⃣ **Set Up DBT**

Install DBT with the following command:

```bash
pip install dbt
```

#### Configure DBT
1. Navigate to your project directory and initialize a new DBT project:
   ```bash
   dbt init learndbt
   ```
2. Update the `~/.dbt/profiles.yml` file with your PostgreSQL configuration:

```yaml
learndbt:
  outputs:
    dev_postgres:
      type: postgres
      host: localhost
      user: admin
      password: admin
      port: 5432
      dbname: learndbt
      schema: public
      threads: 4
  target: dev_postgres
```


---

### 5️⃣ **Run DBT Models**

1. Place your SQL models in the `models/` directory of your DBT project.
2. Run the DBT pipeline to process models:

```bash
dbt run
```

3. Verify the results in your PostgreSQL database by querying the processed tables.

---

## 📂 **Repository Structure**

```plaintext
.
├── archive.zip               # Input data archive
├── postgres_dataloader.py    # Python script for data preprocessing and loading
├── dbt_project/              # DBT project folder
├── README.md                 # Project documentation
```

---

## 📊 **Visualization**

```plaintext
+-----------------------+
| PostgreSQL (Docker)   |
|                       |
| +-------------------+ |
| | Raw Data (440k)   | |
| +-------------------+ |
+----------+------------+
           |
           v
+-----------------------+
| Python Preprocessing  |
|                       |
| +-------------------+ |
| | Clean Data        | |
| +-------------------+ |
+----------+------------+
           |
           v
+-----------------------+
|       DBT Models      |
|                       |
| +-------------------+ |
| | Transformed Data  | |
| +-------------------+ |
+-----------------------+
```

---

## 🌟 **Key Features**
- **Automated pipeline** for processing large datasets.
- Easy integration with Docker, PostgreSQL, and DBT.
- Clean and maintainable code for scalability.

---

## 📌 **Requirements**
- Docker
- Python 3.x
- PostgreSQL
- DBT

---

Feel free to customize this as per your project's needs! Let me know if you want to include any additional details.
